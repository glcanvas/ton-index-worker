#include "IndexScheduler.h"
#include "td/utils/Time.h"
#include "td/utils/StringBuilder.h"
#include <iostream>


std::string get_time_string(double seconds) {
    int days = int(seconds / (60 * 60 * 24));
    int hours = int(seconds / (60 * 60)) % 24;
    int mins = int(seconds / 60) % 60;
    int secs = int(seconds) % 60;

    td::StringBuilder builder;
    bool flag = false;
    if (days > 0) {
        builder << days << "d ";
        flag = true;
    }
    if (flag || (hours > 0)) {
        builder << hours << "h ";
        flag = true;
    }
    if (flag || (mins > 0)) {
        builder << mins << "m ";
        flag = true;
    }
    builder << secs << "s";
    return builder.as_cslice().str();
}

void MasterAddressLoader::run() {
    LOG(DEBUG) << "Start run";

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
            std::_Exit(1);
        }
        td::actor::send_closure(SelfId, &MasterAddressLoader::handle_as_seqno, R.move_as_ok());
    });
    //P.set_value(20);
    td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));
}

void MasterAddressLoader::handle_as_seqno(uint32_t mc_seqno) {
    LOG(DEBUG) << "Do handle as seqno:" << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<MasterchainBlockDataState> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to do:" << R.move_as_error();
            std::_Exit(1);
        }
        td::actor::send_closure(SelfId, &MasterAddressLoader::do_parse_and_insert, R.move_as_ok());
    });
//    P.set_value(MasterchainBlockDataState{
//        // todo fill lib
//    });
    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, mc_seqno, std::move(P));
}

void MasterAddressLoader::do_parse_and_insert(MasterchainBlockDataState state) {
    LOG(DEBUG) << "do_parse_and_insert";
    ParsedBlockPtr ptr = std::make_shared<ParsedBlock>();
    ptr->mc_block_ = state;
    auto states = std::vector<schema::AccountState>();

    for (auto i = this->masterStates.begin(); i != this->masterStates.end(); i++) {
        auto st = schema::AccountState{};
        st.account = block::StdAddress((*i)["address"]["account_address"].asString());
        st.account_status = (*i)["account_state"]["status"].asString();
        std::transform(st.account_status.begin(), st.account_status.end(), st.account_status.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (st.account_status != "active") {
            LOG(WARNING) << "Account: " << st.account << " has non active state: " << st.account_status;
            continue;
        }
        st.code = vm::std_boc_deserialize(td::base64_decode(td::Slice((*i)["account_state"]["code"].asString())).move_as_ok()).move_as_ok();
        st.data = vm::std_boc_deserialize(td::base64_decode(td::Slice((*i)["account_state"]["data"].asString())).move_as_ok()).move_as_ok();
        st.last_trans_lt = (*i)["last_transaction_id"]["lt"].asUInt64();
        states.push_back(std::move(st));
    }
    LOG(DEBUG) << states.size();
    LOG(DEBUG) << this->masterStates[0].toStyledString();
    ptr->account_states_ = states;
    seqno_parsed(ptr);
    //td::actor::send_closure(actor_id(this), &MasterAddressLoader::seqno_parsed, std::move(ptr));

    // LOG(DEBUG) << "OK";
    // auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ParsedBlockPtr> R) {
    //     if (R.is_error()) {
    //         LOG(ERROR) << "Failed to parse synthetic block: " << R.move_as_error();
    //         std::_Exit(1);
    //     }
    //     td::actor::send_closure(SelfId, &MasterAddressLoader::seqno_parsed, R.move_as_ok());
    // });
    // td::actor::send_closure(parse_manager_, &ParseManager::parse, 0, std::move(state), std::move(P));
}

void MasterAddressLoader::seqno_parsed(ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Parsed seqno";

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), parsed_block](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to process interfaces: " << R.move_as_error();
            std::_Exit(1);
        }
        td::actor::send_closure(SelfId, &MasterAddressLoader::seqno_interfaces_processed, std::move(parsed_block));
    });
    td::actor::send_closure(event_processor_, &EventProcessor::process, std::move(parsed_block), std::move(P));
}

void MasterAddressLoader::seqno_interfaces_processed(ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Interfaces processed";

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to insert: " << R.move_as_error();
            std::_Exit(1);
            return;
        }
        LOG(DEBUG) << "Inserted ok";
    });
    auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<QueueState> R) {
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::insert, 0, std::move(parsed_block),
                            std::move(Q), std::move(P));
}