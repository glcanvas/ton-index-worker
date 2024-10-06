#pragma once
#include <queue>
#include <json/value.h>

#include "td/actor/actor.h"

#include "IndexData.h"
#include "DbScanner.h"
#include "EventProcessor.h"
#include "InsertManager.h"
#include "DataParser.h"


class MasterAddressLoader : public td::actor::Actor {
private:
  std::vector<Json::Value> masterStates;
  td::actor::ActorId<DbScanner> db_scanner_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
  td::actor::ActorId<ParseManager> parse_manager_;
  td::actor::ActorOwn<EventProcessor> event_processor_;

public:
  MasterAddressLoader(
    std::vector<Json::Value> values,
    td::actor::ActorId<DbScanner> db_scanner,
    td::actor::ActorId<InsertManagerInterface> insert_manager,
    td::actor::ActorId<ParseManager> parse_manager)
    : masterStates(values), db_scanner_(db_scanner),
      insert_manager_(insert_manager), parse_manager_(parse_manager) {
    event_processor_ = td::actor::create_actor<EventProcessor>("event_processor", insert_manager_);
  };

  void run();

private:
  void handle_as_seqno(uint32_t mc_seqno);

  void do_parse_and_insert(MasterchainBlockDataState state);

  void seqno_parsed(ParsedBlockPtr parsed_block);

  void seqno_interfaces_processed(ParsedBlockPtr parsed_block);

  void seqno_queued_to_insert(QueueState status);

  void seqno_inserted(td::Unit result);
};
