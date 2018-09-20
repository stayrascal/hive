package org.apache.hive.service.cli.history;


import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;

import java.util.Optional;

public interface ExecuteRecordService {
    ExecuteRecord saveExecuteRecord(String sql);

    ExecuteRecord updateExecuteRecord(PersistentEphemeralNode node, ExecuteRecord record);

    Optional<ExecuteRecord> getExecuteRecordBySql(String sql);
}
