/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index;

import org.apache.lucene.index.NoMergePolicy;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTests extends OpenSearchTestCase {
    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    public void testCompoundFileSettings() throws IOException {
        assertThat(new MergePolicyConfig(logger, indexSettings(Settings.EMPTY)).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(true))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0.5))).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(1.0))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("true"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("True"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("False"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("false"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(false))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0.0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
    }

    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testNoMerges() {
        MergePolicyConfig mp = new MergePolicyConfig(
            logger,
            indexSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build())
        );
        assertTrue(mp.getMergePolicy() instanceof NoMergePolicy);
    }

    public void testUpdateSettings() throws IOException {
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
        assertThat(indexSettings.getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        indexSettings = indexSettings(build(0.9));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.9));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.1)));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.1));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.0)));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("true")));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(1.0));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("false")));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
    }

    public void testTieredMergePolicySettingsUpdate() throws IOException {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED,
            0.0d
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d,
            0.0d
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(),
            MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMbFrac(),
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(),
                        new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB)
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB).getMbFrac(),
            0.001
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT - 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT - 1
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(),
            MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getMbFrac(),
            0.0001
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(),
                        new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1)
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1).getMbFrac(),
            0.0001
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1,
            0
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_DELETES_PCT_ALLOWED,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 22).build()
            )
        );
        assertEquals(((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getDeletesPctAllowed(), 22, 0);

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> indexSettings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 53).build()
                )
            )
        );
        final Throwable cause = exc.getCause();
        assertThat(cause.getMessage(), containsString("must be <= 50.0"));
        indexSettings.updateIndexMetadata(newIndexMeta("index", EMPTY_SETTINGS)); // see if defaults are restored
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED,
            0.0d
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb(), ByteSizeUnit.MB).getMbFrac(),
            0.00
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1).getMbFrac(),
            0.0001
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER,
            0
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy()).getDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_DELETES_PCT_ALLOWED,
            0
        );
    }

    public Settings build(String value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(double value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(int value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(boolean value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

}
