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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.dao.SourceRepository;
import feast.core.job.JobManager;
import feast.core.job.JobUpdateTask;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.proto.core.CoreServiceProto.ListStoresRequest.Filter;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto.FeatureSetStatus;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.validation.constraints.Positive;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class JobCoordinatorService {

  private final JobRepository jobRepository;
  private final FeatureSetRepository featureSetRepository;
  private final SourceRepository sourceRepository;
  private final SpecService specService;
  private final JobManager jobManager;
  private final JobProperties jobProperties;

  @Autowired
  public JobCoordinatorService(
      JobRepository jobRepository,
      FeatureSetRepository featureSetRepository,
      SourceRepository sourceRepository,
      SpecService specService,
      JobManager jobManager,
      FeastProperties feastProperties) {
    this.jobRepository = jobRepository;
    this.featureSetRepository = featureSetRepository;
    this.sourceRepository = sourceRepository;
    this.specService = specService;
    this.jobManager = jobManager;
    this.jobProperties = feastProperties.getJobs();
  }

  /**
   * Poll does the following:
   *
   * <p>1) Checks DB and extracts jobs that have to run based on the specs available
   *
   * <p>2) Does a diff with the current set of jobs, starts/updates job(s) if necessary
   *
   * <p>3) Updates job object in DB with status, feature sets
   *
   * <p>4) Updates Feature set statuses
   */
  @Transactional
  @Scheduled(fixedDelayString = "${feast.jobs.polling_interval_milliseconds}")
  public void Poll() throws InvalidProtocolBufferException {
    log.info("Polling for new jobs...");
    @Positive long updateTimeout = jobProperties.getJobUpdateTimeoutSeconds();
    List<JobUpdateTask> jobUpdateTasks = new ArrayList<>();
    ListStoresResponse listStoresResponse = specService.listStores(Filter.newBuilder().build());

    for (StoreProto.Store storeSpec : listStoresResponse.getStoreList()) {
      Set<FeatureSet> featureSets = new HashSet<>();
      Store store = Store.fromProto(storeSpec);

      for (Subscription subscription : store.getSubscriptions()) {
        List<FeatureSet> featureSetsForSub =
            featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc(
                subscription.getName().replace('*', '%'),
                subscription.getProject().replace('*', '%'));
        featureSets.addAll(featureSetsForSub);
      }

      featureSets.stream()
          .collect(Collectors.groupingBy(FeatureSet::getSource))
          .forEach(
              (source, setsForSource) -> {
                // Sources with same type and config in different Feature Sets are different
                // objects.
                // Make sure that we are dealing with the same source object when spawning jobs.
                source = getSurogateSource(source);

                Optional<Job> originalJob = getJob(source, store);
                jobUpdateTasks.add(
                    new JobUpdateTask(
                        setsForSource, source, store, originalJob, jobManager, updateTimeout));
              });
    }
    if (jobUpdateTasks.isEmpty()) {
      log.info("No jobs found.");
      return;
    }

    log.info("Creating/Updating {} jobs...", jobUpdateTasks.size());
    startOrUpdateJobs(jobUpdateTasks);

    log.info("Updating feature set status");
    updateFeatureSetStatuses(jobUpdateTasks);
  }

  void startOrUpdateJobs(List<JobUpdateTask> tasks) {
    ExecutorService executorService = Executors.newFixedThreadPool(tasks.size());
    ExecutorCompletionService<Job> ecs = new ExecutorCompletionService<>(executorService);
    tasks.forEach(ecs::submit);

    int completedTasks = 0;
    List<Job> startedJobs = new ArrayList<>();
    while (completedTasks < tasks.size()) {
      try {
        Job job = ecs.take().get();
        if (job != null) {
          startedJobs.add(job);
        }
      } catch (ExecutionException | InterruptedException e) {
        log.warn("Unable to start or update job: {}", e.getMessage());
      }
      completedTasks++;
    }
    jobRepository.saveAll(startedJobs);
    executorService.shutdown();
  }

  // TODO: make this more efficient
  private void updateFeatureSetStatuses(List<JobUpdateTask> jobUpdateTasks) {
    Set<FeatureSet> ready = new HashSet<>();
    Set<FeatureSet> pending = new HashSet<>();
    for (JobUpdateTask task : jobUpdateTasks) {
      getJob(task.getSource(), task.getStore())
          .ifPresent(
              job -> {
                if (job.isRunning()) {
                  ready.addAll(job.getFeatureSets());
                } else {
                  pending.addAll(job.getFeatureSets());
                }
              });
    }
    ready.removeAll(pending);
    ready.forEach(
        fs -> {
          fs.setStatus(FeatureSetStatus.STATUS_READY);
          featureSetRepository.save(fs);
        });
    pending.forEach(
        fs -> {
          fs.setStatus(FeatureSetStatus.STATUS_JOB_STARTING);
          featureSetRepository.save(fs);
        });
    featureSetRepository.flush();
  }

  @Transactional
  public Optional<Job> getJob(Source source, Store store) {
    List<Job> jobs =
        jobRepository.findBySourceIdAndStoreNameOrderByLastUpdatedDesc(
            source.getId(), store.getName());
    jobs = jobs.stream().filter(job -> !job.hasTerminated()).collect(Collectors.toList());
    if (jobs.isEmpty()) {
      return Optional.empty();
    }
    // return the latest
    return Optional.of(jobs.get(0));
  }

  /**
   * Get the surogate source for the given source. Multiple source objects with different ids can
   * share the same source type and config. This returns the definitive source object for sources
   * with the same type and config.
   */
  private Source getSurogateSource(Source source) {
    return sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
        source.getType(), source.getConfig());
  }
}
