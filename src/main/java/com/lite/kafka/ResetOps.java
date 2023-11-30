package com.lite.kafka;

/**
 * 重置 offset
 * todo
 */
public class ResetOps {
    private String groupId;
    private String topic;
    private int partition;
    private long offset;

    public ResetOps() {}

    public ResetOps(String groupId, String topic, int partition, long offset) {
        this.groupId = groupId;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%d,%d", groupId, topic, partition, offset);
    }

    public static ResetOps fromString(String str) {
        String[] split = str.split(",");
        ResetOps resetOps = new ResetOps();
        resetOps.groupId = split[0];
        resetOps.topic = split[1];
        resetOps.partition = Integer.parseInt(split[2]);
        resetOps.offset = Long.parseLong(split[3]);
        return resetOps;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
