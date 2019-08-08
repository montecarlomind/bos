#include "kafka_plugin.hpp"

#include <fc/io/json.hpp>

#include "kafka.hpp"
#include "try_handle.hpp"
#include <boost/algorithm/string.hpp>

namespace eosio {

using namespace std;

namespace bpo = boost::program_options;
using bpo::options_description;
using bpo::variables_map;

using kafka::handle;

enum class compression_codec {
    none,
    gzip,
    snappy,
    lz4
};

std::istream& operator>>(std::istream& in, compression_codec& codec) {
    std::string s;
    in >> s;
    if (s == "none") codec = compression_codec::none;
    else if (s == "gzip") codec = compression_codec::gzip;
    else if (s == "snappy") codec = compression_codec::snappy;
    else if (s == "lz4") codec = compression_codec::lz4;
    else in.setstate(std::ios_base::failbit);
    return in;
}

static appbase::abstract_plugin& _kafka_relay_plugin = app().register_plugin<kafka_plugin>();

kafka_plugin::kafka_plugin() : kafka_(std::make_unique<kafka::kafka>()) {}
kafka_plugin::~kafka_plugin() {}

void kafka_plugin::set_program_options(options_description&, options_description& cfg) {
    cfg.add_options()
            ("kafka-enable", bpo::value<bool>(), "Kafka enable")
            ("kafka-enable-block", bpo::value<bool>()->default_value(false), "Enable send block messages.")
            ("kafka-enable-transaction", bpo::value<bool>()->default_value(false), "Enable send transaction messages.")
            ("kafka-enable-transaction-trace", bpo::value<bool>()->default_value(false), "Enable send transaction-trace messages.")
            ("kafka-enable-action", bpo::value<bool>()->default_value(true), "Enable send action messages.")
            ("kafka-filter-on", bpo::value<vector<string>>()->composing(), "Track actions and push it to kafka when it match receiver:action. In case action is not specified, all actions to specified account are tracked. e.g., eosio:voteproducer")
            ("kafka-broker-list", bpo::value<string>()->default_value("127.0.0.1:9092"), "Kafka initial broker list, formatted as comma separated pairs of host or host:port, e.g., host1:port1,host2:port2")
            ("kafka-block-topic", bpo::value<string>()->default_value("eos.blocks"), "Kafka topic for message `block`")
            ("kafka-transaction-topic", bpo::value<string>()->default_value("eos.txs"), "Kafka topic for message `transaction`")
            ("kafka-transaction-trace-topic", bpo::value<string>()->default_value("eos.txtraces"), "Kafka topic for message `transaction_trace`")
            ("kafka-action-topic", bpo::value<string>()->default_value("eos.actions"), "Kafka topic for message `action`")
            ("kafka-batch-num-messages", bpo::value<unsigned>()->default_value(1024), "Kafka minimum number of messages to wait for to accumulate in the local queue before sending off a message set")
            ("kafka-queue-buffering-max-ms", bpo::value<unsigned>()->default_value(500), "Kafka how long to wait for kafka-batch-num-messages to fill up in the local queue")
            ("kafka-compression-codec", bpo::value<compression_codec>()->value_name("none/gzip/snappy/lz4"), "Kafka compression codec to use for compressing message sets, default is snappy")
            ("kafka-request-required-acks", bpo::value<int>()->default_value(1), "Kafka indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: 0=Broker does not send any response/ack to client, 1=Only the leader broker will need to ack the message, -1=broker will block until message is committed by all in sync replicas (ISRs) or broker's min.insync.replicas setting before sending response")
            ("kafka-message-send-max-retries", bpo::value<unsigned>()->default_value(2), "Kafka how many times to retry sending a failing MessageSet")
            ("kafka-start-block-num", bpo::value<unsigned>()->default_value(1), "Kafka starts syncing from which block number")
            ("kafka-only-irreversible-blocks", bpo::value<bool>()->default_value(false), "Kafka only sync irreversible blocks")
            ("kafka-only-irreversible-txs", bpo::value<bool>()->default_value(true), "Kafka only sync irreversible transactions")
            ("kafka-statistics-interval-ms", bpo::value<unsigned>()->default_value(0), "Kafka statistics emit interval, maximum is 86400000, 0 disables statistics")
            ("kafka-fixed-partition", bpo::value<int>()->default_value(-1), "Kafka specify fixed partition for all topics, -1 disables specify")
            ;
    // TODO: security options
}

void kafka_plugin::plugin_initialize(const variables_map& options) {
    if (not options.count("kafka-enable") || not options.at("kafka-enable").as<bool>()) {
        wlog("kafka_plugin disabled, since no --kafka-enable=true specified");
        return;
    }

    ilog("Initialize kafka plugin");
    configured_ = true;

    string compressionCodec = "snappy";
    if (options.count("kafka-compression-codec")) {
        switch (options.at("kafka-compression-codec").as<compression_codec>()) {
            case compression_codec::none:
                compressionCodec = "none";
                break;
            case compression_codec::gzip:
                compressionCodec = "gzip";
                break;
            case compression_codec::snappy:
                compressionCodec = "snappy";
                break;
            case compression_codec::lz4:
                compressionCodec = "lz4";
                break;
        }
    }

    kafka::Configuration config = {
            {"metadata.broker.list", options.at("kafka-broker-list").as<string>()},
            {"batch.num.messages", options.at("kafka-batch-num-messages").as<unsigned>()},
            {"queue.buffering.max.ms", options.at("kafka-queue-buffering-max-ms").as<unsigned>()},
            {"compression.codec", compressionCodec},
            {"request.required.acks", options.at("kafka-request-required-acks").as<int>()},
            {"message.send.max.retries", options.at("kafka-message-send-max-retries").as<unsigned>()},
            {"socket.keepalive.enable", true}
    };
    auto stats_interval = options.at("kafka-statistics-interval-ms").as<unsigned>();
    if (stats_interval > 0) {
        config.set("statistics.interval.ms", stats_interval);
        config.set_stats_callback([](kafka::KafkaHandleBase& handle, const std::string& json) {
            ilog("kafka stats: ${json}", ("json", json));
        });
    }
    kafka_->set_config(config);
    kafka_->set_topics(
            options.at("kafka-block-topic").as<string>(),
            options.at("kafka-transaction-topic").as<string>(),
            options.at("kafka-transaction-trace-topic").as<string>(),
            options.at("kafka-action-topic").as<string>()
    );

    if (options.at("kafka-fixed-partition").as<int>() >= 0) {
        kafka_->set_partition(options.at("kafka-fixed-partition").as<int>());
    }

    bool only_irreversible_blocks = options.at("kafka-only-irreversible-blocks").as<bool>();
    bool only_irreversible_txs = options.at("kafka-only-irreversible-txs").as<bool>();
    bool enable_blocks = options.at("kafka-enable-block").as<bool>();
    bool enable_transaction = options.at("kafka-enable-transaction").as<bool>();
    bool enable_transaction_trace = options.at("kafka-enable-transaction-trace").as<bool>();
    bool enable_action = options.at("kafka-enable-action").as<bool>();
    kafka_->set_enable(enable_blocks, enable_transaction, enable_transaction_trace, enable_action, only_irreversible_txs);

    if (options.count("kafka-filter-on"))
    {
      auto fo = options.at("kafka-filter-on").as<vector<string>>();
      for (auto &s : fo)
      {
        std::vector<std::string> v;
        boost::split(v, s, boost::is_any_of(":"));
        EOS_ASSERT(v.size() == 2, fc::invalid_arg_exception, "Invalid value ${s} for --kafka-filter-on", ("s", s));

        kafka::FilterEntry fe{v[0], v[1]};
        EOS_ASSERT(fe.account.value, fc::invalid_arg_exception, "Invalid value ${s} for --kafka-filter-on", ("s", s));
        kafka_->add_filter(fe);
      }
    }

    unsigned start_block_num = options.at("kafka-start-block-num").as<unsigned>();

    // add callback to chain_controller config
    chain_plugin_ = app().find_plugin<chain_plugin>();
    auto& chain = chain_plugin_->chain();

    block_conn_ = chain.accepted_block.connect([=](const chain::block_state_ptr& b) {
        if (not start_sync_) {
            if (b->block_num >= start_block_num) start_sync_ = true;
            else return;
        }
        if (only_irreversible_blocks || not enable_blocks) return;

        handle([=] { kafka_->push_block(b, false); }, "push block");
    });
    irreversible_block_conn_ = chain.irreversible_block.connect([=](const chain::block_state_ptr& b) {
        kafka_->set_lib(b->block_num);
        if (not start_sync_) {
            if (b->block_num >= start_block_num) start_sync_ = true;
            else return;
        }
        if (not enable_blocks) return;

        handle([=] { kafka_->push_block(b, true); }, "push irreversible block");
    });
    transaction_conn_ = chain.applied_transaction.connect([=](const chain::transaction_trace_ptr& t) {
        if (not start_sync_) return;
        handle([=] { kafka_->push_transaction_trace(t); }, "push transaction");
    });
}

void kafka_plugin::plugin_startup() {
    if (not configured_) return;
    ilog("Starting kafka_plugin");
    kafka_->start();
    ilog("Started kafka_plugin");
}

void kafka_plugin::plugin_shutdown() {
    if (not configured_) return;
    ilog("Stopping kafka_plugin");

    try {
        block_conn_.disconnect();
        irreversible_block_conn_.disconnect();
        transaction_conn_.disconnect();

        kafka_->stop();
    } catch (const std::exception& e) {
        elog("Exception on kafka_plugin shutdown: ${e}", ("e", e.what()));
    }

    ilog("Stopped kafka_plugin");
}

}
