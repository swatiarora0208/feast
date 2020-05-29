/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.dao.SourceRepository;
import feast.core.job.JobManager;
import feast.core.job.JobMatcher;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetMeta;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class JobCoordinatorServiceTest {

  @Rule public final ExpectedException exception = ExpectedException.none();
  @Mock JobRepository jobRepository;
  @Mock JobManager jobManager;
  @Mock SpecService specService;
  @Mock FeatureSetRepository featureSetRepository;
  @Mock SourceRepository sourceRepository;

  private FeastProperties feastProperties;

  @Before
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);
    feastProperties.setJobs(jobProperties);
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() throws InvalidProtocolBufferException {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            sourceRepository,
            specService,
            jobManager,
            feastProperties);
    jcs.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    StoreProto.Store storeSpec =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
            .build();
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());
    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("*").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            sourceRepository,
            specService,
            jobManager,
            feastProperties);
    jcs.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldGenerateAndSubmitJobsIfAny() throws InvalidProtocolBufferException {
    StoreProto.Store storeSpec =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    SourceProto.Source sourceSpec =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();
    Source source = Source.fromProto(sourceSpec);

    FeatureSetProto.FeatureSet featureSetSpec1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(sourceSpec)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetSpec1);
    FeatureSetProto.FeatureSet featureSetSpec2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(sourceSpec)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetSpec2);
    String extId = "ext";
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    Job expectedInput =
        new Job(
            "",
            "",
            Runner.DATAFLOW,
            source,
            feast.core.model.Store.fromProto(storeSpec),
            Arrays.asList(featureSet1, featureSet2),
            JobStatus.PENDING);

    Job expected =
        new Job(
            "some_id",
            extId,
            Runner.DATAFLOW,
            source,
            feast.core.model.Store.fromProto(storeSpec),
            Arrays.asList(featureSet1, featureSet2),
            JobStatus.RUNNING);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source.getType(), source.getConfig()))
        .thenReturn(source);
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput)))).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            sourceRepository,
            specService,
            jobManager,
            feastProperties);
    jcs.Poll();
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();
    assertThat(actual, equalTo(Collections.singletonList(expected)));
  }

  @Test
  public void shouldGroupJobsBySource() throws InvalidProtocolBufferException {
    StoreProto.Store storeSpec =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    SourceProto.Source source1Spec =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();
    Source source1 = Source.fromProto(source1Spec);
    SourceProto.Source source2Spec =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("other.servers:9092")
                    .build())
            .build();
    Source source2 = Source.fromProto(source2Spec);

    FeatureSetProto.FeatureSet featureSetSpec1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source1Spec)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetSpec1);

    FeatureSetProto.FeatureSet featureSetSpec2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source2Spec)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetSpec2);

    Job expectedInput1 =
        new Job(
            "name1",
            "",
            Runner.DATAFLOW,
            source1,
            feast.core.model.Store.fromProto(storeSpec),
            Arrays.asList(featureSet1),
            JobStatus.PENDING);

    Job expected1 =
        new Job(
            "name1",
            "extId1",
            Runner.DATAFLOW,
            source1,
            feast.core.model.Store.fromProto(storeSpec),
            Arrays.asList(featureSet1),
            JobStatus.RUNNING);

    Job expectedInput2 =
        new Job(
            "",
            "extId2",
            Runner.DATAFLOW,
            source2,
            feast.core.model.Store.fromProto(storeSpec),
            Arrays.asList(featureSet2),
            JobStatus.PENDING);

    Job expected2 =
        new Job(
            "name2",
            "extId2",
            Runner.DATAFLOW,
            source2,
            feast.core.model.Store.fromProto(storeSpec),
            Arrays.asList(featureSet2),
            JobStatus.RUNNING);
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source1.getType(), source1.getConfig()))
        .thenReturn(source1);
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source2.getType(), source2.getConfig()))
        .thenReturn(source2);
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput1)))).thenReturn(expected1);
    when(jobManager.startJob(argThat(new JobMatcher(expectedInput2)))).thenReturn(expected2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            sourceRepository,
            specService,
            jobManager,
            feastProperties);
    jcs.Poll();

    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();

    assertThat(actual.get(0), equalTo(expected1));
    assertThat(actual.get(1), equalTo(expected2));
  }

  @Test
  public void shouldGroupJobsBySourceAndIgnoreDuplicateSourceObjects()
      throws InvalidProtocolBufferException {
    StoreProto.Store storeSpec =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    Store store = Store.fromProto(storeSpec);

    // simulate duplicate source objects: create source objects from the same spec but with
    // different ids
    SourceProto.Source sourceSpec =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();
    Source source1 = Source.fromProto(sourceSpec);
    source1.setId(1);
    Source source2 = Source.fromProto(sourceSpec);
    source2.setId(2);

    FeatureSetProto.FeatureSet featureSetSpec1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(sourceSpec)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetSpec1);
    featureSet1.setSource(source1);

    FeatureSetProto.FeatureSet featureSetSpec2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(sourceSpec)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetSpec2);
    featureSet2.setSource(source2);

    Job expectedInput =
        new Job(
            "",
            "",
            Runner.DATAFLOW,
            source1,
            store,
            List.of(featureSet1, featureSet2),
            JobStatus.PENDING);
    Job expected =
        new Job(
            "name",
            "extid",
            Runner.DATAFLOW,
            source1,
            store,
            List.of(featureSet1, featureSet2),
            JobStatus.RUNNING);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source1.getType(), source1.getConfig()))
        .thenReturn(source1);
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source2.getType(), source2.getConfig()))
        .thenReturn(source1);

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput)))).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            sourceRepository,
            specService,
            jobManager,
            feastProperties);
    jcs.Poll();
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();
    assertThat(actual, equalTo(Collections.singletonList(expected)));
  }
}
