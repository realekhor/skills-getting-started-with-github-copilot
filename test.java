package com.k8s.controller;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kubernetes Helm Release Monitor
 * 
 * Watches for Helm releases (via Secrets with owner=helm) and monitors
 * the associated deployments. Each Helm release gets its own root span.
 * Deployment spans are children of the release root span and are sent
 * to Tempo when deployments become available or stall.
 */
public class K8sReleaseMonitoringController {

    private static final String DEFAULT_SERVICE_NAME = "helm-release-monitor";
    private static String serviceName;
    private static Tracer tracer;
    private static SdkTracerProvider tracerProvider;
    private static KubernetesClient kubernetesClient;
    private static String namespace;
    private static final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private static volatile boolean jobMonitoringEnabled = true;

    // Active Helm releases: key = release-name
    private static final Map<String, HelmReleaseInfo> activeReleases = new ConcurrentHashMap<>();

    // Last processed release version per name to avoid reprocessing
    private static final Map<String, Integer> processedReleaseVersions = new ConcurrentHashMap<>();

    // Tracked deployments: key = release-name/deployment-name
    private static final Map<String, DeploymentTracker> trackedDeployments = new ConcurrentHashMap<>();

    // Tracked jobs: key = release-name/job-name
    private static final Map<String, JobTracker> trackedJobs = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        String otelEndpoint = System.getenv("OTEL_ENDPOINT");
        if (otelEndpoint == null || otelEndpoint.isEmpty()) {
            otelEndpoint = "http://localhost:4317";
        }
        if (!otelEndpoint.startsWith("http://") && !otelEndpoint.startsWith("https://")) {
            otelEndpoint = "http://" + otelEndpoint;
        }

        namespace = System.getenv("NAMESPACE");
        if (namespace == null || namespace.isEmpty()) {
            namespace = "default";
        }

        serviceName = System.getenv("SERVICE_NAME");
        if (serviceName == null || serviceName.isEmpty()) {
            serviceName = DEFAULT_SERVICE_NAME;
        }

        System.out.println("Starting Helm Release Monitor...");
        System.out.println("Service Name: " + serviceName);
        System.out.println("OTEL Endpoint: " + otelEndpoint);
        System.out.println("Watching namespace: " + namespace);

        initializeOpenTelemetry(otelEndpoint);

        Config config = new ConfigBuilder()
                .withRequestTimeout(120000)
                .withConnectionTimeout(60000)
                .build();
        kubernetesClient = new KubernetesClientBuilder().withConfig(config).build();

        // Watch Helm release Secrets (owner=helm)
        SharedIndexInformer<Secret> secretInformer = startWithRetry("Secret",
                K8sReleaseMonitoringController::startHelmReleaseInformer);

        // Watch Deployments for status changes
        SharedIndexInformer<Deployment> deploymentInformer = startWithRetry("Deployment",
                K8sReleaseMonitoringController::startDeploymentInformer);

        // Watch Jobs for completion/failure (optional - requires batch/v1 RBAC permissions)
        SharedIndexInformer<Job> jobInformer = null;
        try {
            jobInformer = startWithRetry("Job",
                    K8sReleaseMonitoringController::startJobInformer);
        } catch (Exception e) {
            jobMonitoringEnabled = false;
            System.err.println("WARNING: Job monitoring disabled - insufficient RBAC permissions to list/watch Jobs.");
            System.err.println("  Grant 'list' and 'watch' on 'jobs' in API group 'batch' to enable job monitoring.");
            System.err.println("  Error: " + e.getMessage());
        }

        final SharedIndexInformer<Job> jobInformerRef = jobInformer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            stopRequested.set(true);
            secretInformer.close();
            deploymentInformer.close();
            if (jobInformerRef != null) jobInformerRef.close();
            kubernetesClient.close();
            if (tracerProvider != null) {
                tracerProvider.forceFlush();
                tracerProvider.close();
            }
        }));

        System.out.println("Controller started, waiting for Helm releases...");

        while (!stopRequested.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private static void initializeOpenTelemetry(String endpoint) {
        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(endpoint)
                .build();

        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        AttributeKey.stringKey("service.name"), serviceName
                )));

        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .setResource(resource)
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        tracer = openTelemetry.getTracer(serviceName);
    }

    // ---- Retry Helper ----

    @SuppressWarnings("unchecked")
    private static <T> SharedIndexInformer<T> startWithRetry(String resourceType,
            java.util.function.Supplier<SharedIndexInformer<?>> starter) {
        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                System.out.printf("Starting %s informer (attempt %d/%d)...%n", resourceType, attempt, maxRetries);
                return (SharedIndexInformer<T>) starter.get();
            } catch (Exception e) {
                System.err.printf("Failed to start %s informer (attempt %d/%d): %s%n",
                        resourceType, attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    throw new RuntimeException("Failed to start " + resourceType + " informer after " + maxRetries + " attempts", e);
                }
                try {
                    long backoff = (long) Math.pow(2, attempt) * 1000;
                    System.out.printf("Retrying in %d ms...%n", backoff);
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry", ie);
                }
            }
        }
        throw new RuntimeException("Unreachable");
    }

    // ---- Helm Release Detection (via Secrets) ----

    private static SharedIndexInformer<Secret> startHelmReleaseInformer() {
        SharedIndexInformer<Secret> informer = kubernetesClient.secrets()
                .inNamespace(namespace)
                .withLabel("owner", "helm")
                .inform(new ResourceEventHandler<Secret>() {
                    @Override
                    public void onAdd(Secret secret) {
                        handleHelmReleaseSecret(secret);
                    }

                    @Override
                    public void onUpdate(Secret oldSecret, Secret newSecret) {
                        handleHelmReleaseSecret(newSecret);
                    }

                    @Override
                    public void onDelete(Secret secret, boolean deletedFinalStateUnknown) {
                        // No action needed
                    }
                }, 30000L);

        while (!informer.hasSynced() && !stopRequested.get()) {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }

        System.out.println("Helm release Secret informer synced.");
        return informer;
    }

    private static void handleHelmReleaseSecret(Secret secret) {
        if (secret.getMetadata() == null) return;
        Map<String, String> labels = secret.getMetadata().getLabels();
        if (labels == null) return;

        String releaseName = labels.get("name");
        String status = labels.get("status");
        String versionStr = labels.get("version");

        if (releaseName == null || status == null || versionStr == null) return;

        // Only react to newly deployed releases
        if (!"deployed".equals(status)) return;

        int version;
        try {
            version = Integer.parseInt(versionStr);
        } catch (NumberFormatException e) {
            return;
        }

        // Skip if we already processed this version
        Integer lastVersion = processedReleaseVersions.get(releaseName);
        if (lastVersion != null && lastVersion >= version) return;

        processedReleaseVersions.put(releaseName, version);

        System.out.printf("Helm release detected: %s (version %d)%n", releaseName, version);

        // End any previous root span for this release (superseded)
        HelmReleaseInfo oldRelease = activeReleases.remove(releaseName);
        if (oldRelease != null && oldRelease.rootSpan.isRecording()) {
            oldRelease.rootSpan.setAttribute("release.status", "superseded");
            oldRelease.rootSpan.end();
        }
        // Clean up old deployment and job tracking for this release
        trackedDeployments.entrySet().removeIf(e -> e.getKey().startsWith(releaseName + "/"));
        trackedJobs.entrySet().removeIf(e -> e.getKey().startsWith(releaseName + "/"));

        // Use the secret's creation timestamp as the release start time
        // This reflects when Helm actually initiated the release, not when the controller detected it
        Instant startTime;
        if (secret.getMetadata().getCreationTimestamp() != null) {
            startTime = Instant.parse(secret.getMetadata().getCreationTimestamp());
        } else {
            startTime = Instant.now();
        }
        Span rootSpan = tracer.spanBuilder(releaseName + "-v" + version)
                .setNoParent()
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(startTime)
                .setAttribute("k8s.namespace", namespace)
                .setAttribute("helm.release.name", releaseName)
                .setAttribute("helm.release.version", version)
                .setAttribute("release.type", "helm-release")
                .setAttribute("release.start.time", startTime.toString())
                .startSpan();

        Context rootContext = Context.root().with(rootSpan);
        HelmReleaseInfo releaseInfo = new HelmReleaseInfo(releaseName, version, startTime, rootSpan, rootContext);
        activeReleases.put(releaseName, releaseInfo);

        System.out.printf("Created root span for %s-v%d, traceId: %s%n",
                releaseName, version, rootSpan.getSpanContext().getTraceId());

        // Scan existing deployments and jobs that belong to this release
        scanExistingDeployments(releaseInfo);
        if (jobMonitoringEnabled) {
            scanExistingJobs(releaseInfo);
        }
    }

    private static void scanExistingDeployments(HelmReleaseInfo releaseInfo) {
        List<Deployment> deployments = kubernetesClient.apps().deployments()
                .inNamespace(namespace)
                .withLabel("app.kubernetes.io/instance", releaseInfo.name)
                .list()
                .getItems();

        System.out.printf("Found %d deployments for release %s%n", deployments.size(), releaseInfo.name);

        for (Deployment deployment : deployments) {
            if (deployment.getMetadata() == null) continue;
            String deploymentName = deployment.getMetadata().getName();
            String trackingKey = releaseInfo.name + "/" + deploymentName;
            Integer desiredReplicas = deployment.getSpec() != null ? deployment.getSpec().getReplicas() : 1;
            Long generation = deployment.getMetadata().getGeneration();
            Map<String, String> labels = deployment.getMetadata().getLabels();
            String chartName = labels != null ? labels.get("app.kubernetes.io/name") : null;

            releaseInfo.deploymentNames.add(deploymentName);

            boolean isAvailable = isDeploymentAvailable(deployment);
            boolean isProgressing = isDeploymentProgressing(deployment);
            boolean isStalled = isDeploymentStalled(deployment);

            // Use the deployment's actual rollout start time from its conditions
            Instant deployStartTime = getDeploymentRolloutStartTime(deployment, releaseInfo.startTime);

            if (isAvailable && !isProgressing) {
                // Already available — trace immediately
                DeploymentTracker tracker = new DeploymentTracker(
                        deploymentName, deployStartTime, desiredReplicas, generation, chartName);
                tracker.traced = true;
                trackedDeployments.put(trackingKey, tracker);

                Integer availableReplicas = deployment.getStatus() != null
                        ? deployment.getStatus().getAvailableReplicas() : 0;

                sendDeploymentTrace(deploymentName, desiredReplicas,
                        availableReplicas != null ? availableReplicas : 0,
                        releaseInfo.name, chartName,
                        deployStartTime, Instant.now(), "available", releaseInfo);

            } else if (isStalled) {
                // Stalled — trace immediately with error status
                DeploymentTracker tracker = new DeploymentTracker(
                        deploymentName, deployStartTime, desiredReplicas, generation, chartName);
                tracker.traced = true;
                trackedDeployments.put(trackingKey, tracker);

                Integer availableReplicas = deployment.getStatus() != null
                        ? deployment.getStatus().getAvailableReplicas() : 0;

                sendDeploymentTrace(deploymentName, desiredReplicas,
                        availableReplicas != null ? availableReplicas : 0,
                        releaseInfo.name, chartName,
                        deployStartTime, Instant.now(), "stalled", releaseInfo);

            } else {
                // Still rolling out — track for monitoring
                trackedDeployments.put(trackingKey, new DeploymentTracker(
                        deploymentName, deployStartTime, desiredReplicas, generation, chartName));
            }
        }

        checkReleaseComplete(releaseInfo.name);
    }

    private static void scanExistingJobs(HelmReleaseInfo releaseInfo) {
        List<Job> jobs;
        try {
            jobs = kubernetesClient.batch().v1().jobs()
                    .inNamespace(namespace)
                    .withLabel("app.kubernetes.io/instance", releaseInfo.name)
                    .list()
                    .getItems();
        } catch (Exception e) {
            System.err.printf("WARNING: Failed to list jobs for release %s (RBAC?): %s%n",
                    releaseInfo.name, e.getMessage());
            jobMonitoringEnabled = false;
            return;
        }

        System.out.printf("Found %d jobs for release %s%n", jobs.size(), releaseInfo.name);

        for (Job job : jobs) {
            if (job.getMetadata() == null) continue;
            String jobName = job.getMetadata().getName();
            String trackingKey = releaseInfo.name + "/" + jobName;
            Map<String, String> labels = job.getMetadata().getLabels();
            String chartName = labels != null ? labels.get("app.kubernetes.io/name") : null;

            releaseInfo.jobNames.add(jobName);

            String jobStatus = getJobStatus(job);
            if ("complete".equals(jobStatus) || "failed".equals(jobStatus)) {
                JobTracker tracker = new JobTracker(jobName, releaseInfo.startTime, chartName);
                tracker.traced = true;
                trackedJobs.put(trackingKey, tracker);

                Instant jobStartTime = getJobStartTime(job, releaseInfo.startTime);
                Instant jobEndTime = getJobCompletionTime(job, Instant.now());

                sendJobTrace(jobName, chartName, jobStartTime, jobEndTime, jobStatus, releaseInfo);
            } else {
                trackedJobs.put(trackingKey, new JobTracker(jobName, Instant.now(), chartName));
            }
        }
    }

    // ---- Deployment Monitoring ----

    private static SharedIndexInformer<Deployment> startDeploymentInformer() {
        SharedIndexInformer<Deployment> informer = kubernetesClient.apps().deployments()
                .inNamespace(namespace)
                .inform(new ResourceEventHandler<Deployment>() {
                    @Override
                    public void onAdd(Deployment deployment) {
                        // Ignore initial sync — we scan on release detection
                    }

                    @Override
                    public void onUpdate(Deployment oldDeployment, Deployment newDeployment) {
                        handleDeploymentUpdate(newDeployment);
                    }

                    @Override
                    public void onDelete(Deployment deployment, boolean deletedFinalStateUnknown) {
                        if (deployment.getMetadata() == null) return;
                        String name = deployment.getMetadata().getName();
                        trackedDeployments.keySet().removeIf(k -> k.endsWith("/" + name));
                        System.out.println("Deployment DELETED: " + name);
                    }
                }, 30000L);

        while (!informer.hasSynced() && !stopRequested.get()) {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }

        System.out.println("Deployment informer synced.");
        return informer;
    }

    private static void handleDeploymentUpdate(Deployment deployment) {
        if (deployment.getMetadata() == null) return;

        String deploymentName = deployment.getMetadata().getName();
        Map<String, String> labels = deployment.getMetadata().getLabels();
        String helmInstance = labels != null ? labels.get("app.kubernetes.io/instance") : null;

        // Only process deployments belonging to an active Helm release
        if (helmInstance == null) return;
        HelmReleaseInfo releaseInfo = activeReleases.get(helmInstance);
        if (releaseInfo == null) return;

        String trackingKey = helmInstance + "/" + deploymentName;
        Long generation = deployment.getMetadata().getGeneration();
        Integer desiredReplicas = deployment.getSpec() != null ? deployment.getSpec().getReplicas() : 1;
        String chartName = labels.get("app.kubernetes.io/name");

        DeploymentStatus depStatus = deployment.getStatus();
        if (depStatus == null) return;

        boolean isProgressing = isDeploymentProgressing(deployment);
        boolean isAvailable = isDeploymentAvailable(deployment);
        boolean isStalled = isDeploymentStalled(deployment);
        Integer availableReplicas = depStatus.getAvailableReplicas();

        // Register deployment if not yet tracked under this release
        if (!trackedDeployments.containsKey(trackingKey)) {
            releaseInfo.deploymentNames.add(deploymentName);
            Instant deployStartTime = getDeploymentRolloutStartTime(deployment, Instant.now());
            trackedDeployments.put(trackingKey, new DeploymentTracker(
                    deploymentName, deployStartTime, desiredReplicas, generation, chartName));
        }

        DeploymentTracker tracker = trackedDeployments.get(trackingKey);
        if (tracker.traced) return;

        // Detect new generation (new rollout within same release)
        if (!tracker.generation.equals(generation)) {
            tracker.startTime = getDeploymentRolloutStartTime(deployment, Instant.now());
            tracker.generation = generation;
            tracker.traced = false;
        }

        System.out.printf("Deployment %s: available=%s, progressing=%s, stalled=%s, replicas=%d/%d%n",
                deploymentName, isAvailable, isProgressing, isStalled,
                availableReplicas != null ? availableReplicas : 0, desiredReplicas);

        if ((isAvailable && !isProgressing) || isStalled) {
            tracker.traced = true;
            String finalStatus = isStalled ? "stalled" : "available";

            sendDeploymentTrace(deploymentName, desiredReplicas,
                    availableReplicas != null ? availableReplicas : 0,
                    helmInstance, chartName,
                    tracker.startTime, Instant.now(), finalStatus, releaseInfo);

            checkReleaseComplete(helmInstance);
        }
    }

    // ---- Job Monitoring ----

    private static SharedIndexInformer<Job> startJobInformer() {
        SharedIndexInformer<Job> informer = kubernetesClient.batch().v1().jobs()
                .inNamespace(namespace)
                .inform(new ResourceEventHandler<Job>() {
                    @Override
                    public void onAdd(Job job) {
                        // Ignore initial sync — we scan on release detection
                    }

                    @Override
                    public void onUpdate(Job oldJob, Job newJob) {
                        handleJobUpdate(newJob);
                    }

                    @Override
                    public void onDelete(Job job, boolean deletedFinalStateUnknown) {
                        if (job.getMetadata() == null) return;
                        String name = job.getMetadata().getName();
                        trackedJobs.keySet().removeIf(k -> k.endsWith("/" + name));
                        System.out.println("Job DELETED: " + name);
                    }
                }, 30000L);

        while (!informer.hasSynced() && !stopRequested.get()) {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }

        System.out.println("Job informer synced.");
        return informer;
    }

    private static void handleJobUpdate(Job job) {
        if (job.getMetadata() == null) return;

        String jobName = job.getMetadata().getName();
        Map<String, String> labels = job.getMetadata().getLabels();
        String helmInstance = labels != null ? labels.get("app.kubernetes.io/instance") : null;

        if (helmInstance == null) return;
        HelmReleaseInfo releaseInfo = activeReleases.get(helmInstance);
        if (releaseInfo == null) return;

        String trackingKey = helmInstance + "/" + jobName;
        String chartName = labels.get("app.kubernetes.io/name");

        String jobStatus = getJobStatus(job);
        if (jobStatus == null) return; // still running

        if (!trackedJobs.containsKey(trackingKey)) {
            releaseInfo.jobNames.add(jobName);
            trackedJobs.put(trackingKey, new JobTracker(jobName, Instant.now(), chartName));
        }

        JobTracker tracker = trackedJobs.get(trackingKey);
        if (tracker.traced) return;

        System.out.printf("Job %s: status=%s%n", jobName, jobStatus);

        if ("complete".equals(jobStatus) || "failed".equals(jobStatus)) {
            tracker.traced = true;

            Instant jobStartTime = getJobStartTime(job, tracker.startTime);
            Instant jobEndTime = getJobCompletionTime(job, Instant.now());

            sendJobTrace(jobName, chartName, jobStartTime, jobEndTime, jobStatus, releaseInfo);
            checkReleaseComplete(helmInstance);
        }
    }

    private static String getJobStatus(Job job) {
        JobStatus status = job.getStatus();
        if (status == null || status.getConditions() == null) return null;

        for (JobCondition condition : status.getConditions()) {
            if ("Complete".equals(condition.getType()) && "True".equals(condition.getStatus())) {
                return "complete";
            }
            if ("Failed".equals(condition.getType()) && "True".equals(condition.getStatus())) {
                return "failed";
            }
        }
        return null;
    }

    private static Instant getJobStartTime(Job job, Instant fallback) {
        if (job.getStatus() != null && job.getStatus().getStartTime() != null) {
            try {
                return Instant.parse(job.getStatus().getStartTime());
            } catch (Exception e) {
                // fall through
            }
        }
        return fallback;
    }

    private static Instant getJobCompletionTime(Job job, Instant fallback) {
        if (job.getStatus() != null && job.getStatus().getCompletionTime() != null) {
            try {
                return Instant.parse(job.getStatus().getCompletionTime());
            } catch (Exception e) {
                // fall through
            }
        }
        return fallback;
    }

    // ---- Release Completion ----

    private static void checkReleaseComplete(String releaseName) {
        HelmReleaseInfo releaseInfo = activeReleases.get(releaseName);
        if (releaseInfo == null || releaseInfo.deploymentNames.isEmpty()) return;

        for (String depName : releaseInfo.deploymentNames) {
            DeploymentTracker tracker = trackedDeployments.get(releaseName + "/" + depName);
            if (tracker == null || !tracker.traced) return;
        }
        for (String jobName : releaseInfo.jobNames) {
            JobTracker tracker = trackedJobs.get(releaseName + "/" + jobName);
            if (tracker == null || !tracker.traced) return;
        }

        // All deployments for this release are done
        Instant endTime = Instant.now();
        Duration duration = Duration.between(releaseInfo.startTime, endTime);

        releaseInfo.rootSpan.setAttribute("release.end.time", endTime.toString());
        releaseInfo.rootSpan.setAttribute("release.duration.ms", duration.toMillis());
        releaseInfo.rootSpan.setAttribute("release.deployment.count", releaseInfo.deploymentNames.size());
        releaseInfo.rootSpan.setAttribute("release.job.count", releaseInfo.jobNames.size());
        releaseInfo.rootSpan.setAttribute("release.status", "complete");
        releaseInfo.rootSpan.end(endTime);

        System.out.printf("Helm release %s-v%d complete. %d deployments, %d jobs traced in %s. traceId: %s%n",
                releaseName, releaseInfo.version, releaseInfo.deploymentNames.size(),
                releaseInfo.jobNames.size(), formatDuration(duration),
                releaseInfo.rootSpan.getSpanContext().getTraceId());

        activeReleases.remove(releaseName);
        trackedDeployments.entrySet().removeIf(e -> e.getKey().startsWith(releaseName + "/"));
        trackedJobs.entrySet().removeIf(e -> e.getKey().startsWith(releaseName + "/"));
    }

    // ---- Trace Sending ----

    private static void sendDeploymentTrace(String deploymentName, int desiredReplicas,
            int availableReplicas, String helmReleaseName, String helmChartName,
            Instant startTime, Instant endTime, String status, HelmReleaseInfo releaseInfo) {

        Duration duration = Duration.between(startTime, endTime);

        var spanBuilder = tracer.spanBuilder(deploymentName)
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(startTime)
                .setAttribute("k8s.deployment.name", deploymentName)
                .setAttribute("k8s.deployment.namespace", namespace)
                .setAttribute("k8s.deployment.replicas.desired", desiredReplicas)
                .setAttribute("k8s.deployment.replicas.available", availableReplicas)
                .setAttribute("k8s.deployment.status", status)
                .setAttribute("k8s.deployment.duration.ms", duration.toMillis())
                .setAttribute("k8s.deployment.start.time", startTime.toString())
                .setAttribute("k8s.deployment.end.time", endTime.toString())
                .setAttribute("k8s.deployment.helm.managed", true);

        // Set parent to the release root span
        spanBuilder.setParent(releaseInfo.rootContext);

        if (helmReleaseName != null && !helmReleaseName.isEmpty()) {
            spanBuilder.setAttribute("helm.release.name", helmReleaseName);
        }
        if (helmChartName != null && !helmChartName.isEmpty()) {
            spanBuilder.setAttribute("helm.chart.name", helmChartName);
        }

        Span span = spanBuilder.startSpan();

        if ("stalled".equals(status)) {
            span.setStatus(StatusCode.ERROR, "Deployment stalled - ProgressDeadlineExceeded");
        }

        span.end(endTime);

        System.out.printf("Traced deployment %s [%s] under release %s, traceId: %s, duration: %s%n",
                deploymentName, status, helmReleaseName,
                span.getSpanContext().getTraceId(), formatDuration(duration));
    }

    private static void sendJobTrace(String jobName, String helmChartName,
            Instant startTime, Instant endTime, String status, HelmReleaseInfo releaseInfo) {

        Duration duration = Duration.between(startTime, endTime);

        var spanBuilder = tracer.spanBuilder(jobName)
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(startTime)
                .setAttribute("k8s.job.name", jobName)
                .setAttribute("k8s.job.namespace", namespace)
                .setAttribute("k8s.job.status", status)
                .setAttribute("k8s.job.duration.ms", duration.toMillis())
                .setAttribute("k8s.job.start.time", startTime.toString())
                .setAttribute("k8s.job.end.time", endTime.toString())
                .setAttribute("k8s.job.helm.managed", true);

        spanBuilder.setParent(releaseInfo.rootContext);

        if (releaseInfo.name != null && !releaseInfo.name.isEmpty()) {
            spanBuilder.setAttribute("helm.release.name", releaseInfo.name);
        }
        if (helmChartName != null && !helmChartName.isEmpty()) {
            spanBuilder.setAttribute("helm.chart.name", helmChartName);
        }

        Span span = spanBuilder.startSpan();

        if ("failed".equals(status)) {
            span.setStatus(StatusCode.ERROR, "Job failed");
        }

        span.end(endTime);

        System.out.printf("Traced job %s [%s] under release %s, traceId: %s, duration: %s%n",
                jobName, status, releaseInfo.name,
                span.getSpanContext().getTraceId(), formatDuration(duration));
    }

    // ---- Deployment Status Checks ----

    private static boolean isDeploymentProgressing(Deployment deployment) {
        if (deployment.getStatus() == null || deployment.getStatus().getConditions() == null) {
            return true;
        }

        for (DeploymentCondition condition : deployment.getStatus().getConditions()) {
            if ("Progressing".equals(condition.getType())) {
                String reason = condition.getReason();
                if ("NewReplicaSetCreated".equals(reason) ||
                    "FoundNewReplicaSet".equals(reason) ||
                    "ReplicaSetUpdated".equals(reason)) {
                    return true;
                }
                if ("NewReplicaSetAvailable".equals(reason)) {
                    return false;
                }
            }
        }

        Long observedGeneration = deployment.getStatus().getObservedGeneration();
        Long generation = deployment.getMetadata().getGeneration();
        if (observedGeneration != null && generation != null && !observedGeneration.equals(generation)) {
            return true;
        }

        return false;
    }

    private static boolean isDeploymentAvailable(Deployment deployment) {
        if (deployment.getStatus() == null) return false;

        if (deployment.getStatus().getConditions() != null) {
            for (DeploymentCondition condition : deployment.getStatus().getConditions()) {
                if ("Available".equals(condition.getType())) {
                    return "True".equals(condition.getStatus());
                }
            }
        }

        Integer desiredReplicas = deployment.getSpec() != null ? deployment.getSpec().getReplicas() : 1;
        Integer availableReplicas = deployment.getStatus().getAvailableReplicas();
        Integer readyReplicas = deployment.getStatus().getReadyReplicas();
        Integer updatedReplicas = deployment.getStatus().getUpdatedReplicas();

        return availableReplicas != null && availableReplicas >= desiredReplicas &&
               readyReplicas != null && readyReplicas >= desiredReplicas &&
               updatedReplicas != null && updatedReplicas >= desiredReplicas;
    }

    private static boolean isDeploymentStalled(Deployment deployment) {
        if (deployment.getStatus() == null || deployment.getStatus().getConditions() == null) return false;

        for (DeploymentCondition condition : deployment.getStatus().getConditions()) {
            if ("Progressing".equals(condition.getType()) &&
                "False".equals(condition.getStatus()) &&
                "ProgressDeadlineExceeded".equals(condition.getReason())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the deployment's actual rollout start time from its Progressing condition's
     * lastTransitionTime, falling back to the deployment's creation timestamp.
     * This ensures child deployment spans reflect when the rollout actually began,
     * not when the Helm release was created.
     */
    private static Instant getDeploymentRolloutStartTime(Deployment deployment, Instant fallback) {
        if (deployment.getStatus() != null && deployment.getStatus().getConditions() != null) {
            for (DeploymentCondition condition : deployment.getStatus().getConditions()) {
                if ("Progressing".equals(condition.getType()) && condition.getLastTransitionTime() != null) {
                    try {
                        return Instant.parse(condition.getLastTransitionTime());
                    } catch (Exception e) {
                        // fall through
                    }
                }
            }
        }
        // Fall back to deployment creation timestamp
        if (deployment.getMetadata() != null && deployment.getMetadata().getCreationTimestamp() != null) {
            try {
                return Instant.parse(deployment.getMetadata().getCreationTimestamp());
            } catch (Exception e) {
                // fall through
            }
        }
        return fallback;
    }

    private static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds < 60) return seconds + "s";
        return String.format("%dm %ds", seconds / 60, seconds % 60);
    }

    // ---- Inner Classes ----

    private static class HelmReleaseInfo {
        final String name;
        final int version;
        final Instant startTime;
        final Span rootSpan;
        final Context rootContext;
        final Set<String> deploymentNames = ConcurrentHashMap.newKeySet();
        final Set<String> jobNames = ConcurrentHashMap.newKeySet();

        HelmReleaseInfo(String name, int version, Instant startTime, Span rootSpan, Context rootContext) {
            this.name = name;
            this.version = version;
            this.startTime = startTime;
            this.rootSpan = rootSpan;
            this.rootContext = rootContext;
        }
    }

    private static class DeploymentTracker {
        final String name;
        Instant startTime;
        final int desiredReplicas;
        Long generation;
        final String chartName;
        volatile boolean traced;

        DeploymentTracker(String name, Instant startTime, int desiredReplicas, Long generation, String chartName) {
            this.name = name;
            this.startTime = startTime;
            this.desiredReplicas = desiredReplicas;
            this.generation = generation;
            this.chartName = chartName;
            this.traced = false;
        }
    }

    private static class JobTracker {
        final String name;
        final Instant startTime;
        final String chartName;
        volatile boolean traced;

        JobTracker(String name, Instant startTime, String chartName) {
            this.name = name;
            this.startTime = startTime;
            this.chartName = chartName;
            this.traced = false;
        }
    }
}
