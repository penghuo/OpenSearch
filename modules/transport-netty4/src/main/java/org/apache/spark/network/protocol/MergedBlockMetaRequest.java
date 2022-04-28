/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

/**
 * Request to find the meta information for the specified merged block. The meta information
 * contains the number of chunks in the merged blocks and the maps ids in each chunk.
 *
 * @since 3.2.0
 */
public class MergedBlockMetaRequest extends AbstractMessage implements RequestMessage {
    public final long requestId;
    public final String appId;
    public final int shuffleId;
    public final int shuffleMergeId;
    public final int reduceId;

    public MergedBlockMetaRequest(long requestId, String appId, int shuffleId, int shuffleMergeId, int reduceId) {
        super(null, false);
        this.requestId = requestId;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.shuffleMergeId = shuffleMergeId;
        this.reduceId = reduceId;
    }

    @Override
    public Type type() {
        return Type.MergedBlockMetaRequest;
    }

    @Override
    public int encodedLength() {
        return 8 + Encoders.Strings.encodedLength(appId) + 4 + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        Encoders.Strings.encode(buf, appId);
        buf.writeInt(shuffleId);
        buf.writeInt(shuffleMergeId);
        buf.writeInt(reduceId);
    }

    public static MergedBlockMetaRequest decode(ByteBuf buf) {
        long requestId = buf.readLong();
        String appId = Encoders.Strings.decode(buf);
        int shuffleId = buf.readInt();
        int shuffleMergeId = buf.readInt();
        int reduceId = buf.readInt();
        return new MergedBlockMetaRequest(requestId, appId, shuffleId, shuffleMergeId, reduceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, appId, shuffleId, shuffleMergeId, reduceId);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof MergedBlockMetaRequest) {
            MergedBlockMetaRequest o = (MergedBlockMetaRequest) other;
            return requestId == o.requestId
                && shuffleId == o.shuffleId
                && shuffleMergeId == o.shuffleMergeId
                && reduceId == o.reduceId
                && Objects.equals(appId, o.appId);
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("requestId", requestId)
            .append("appId", appId)
            .append("shuffleId", shuffleId)
            .append("shuffleMergeId", shuffleMergeId)
            .append("reduceId", reduceId)
            .toString();
    }
}
