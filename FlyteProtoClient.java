package com.google.tsunami.plugins.rce;

import java.net.URI;
import java.util.Arrays;

import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.Duration;
import com.google.protobuf.Struct;

import flyteidl.admin.ExecutionOuterClass.Execution;
import flyteidl.admin.ExecutionOuterClass.ExecutionCreateRequest;
import flyteidl.admin.ExecutionOuterClass.ExecutionCreateResponse;
import flyteidl.admin.ExecutionOuterClass.ExecutionMetadata;
import flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode;
import flyteidl.admin.ExecutionOuterClass.ExecutionSpec;
import flyteidl.admin.ExecutionOuterClass.WorkflowExecutionGetRequest;
import flyteidl.admin.ProjectOuterClass;
import flyteidl.admin.ProjectOuterClass.Project;
import flyteidl.admin.ProjectOuterClass.ProjectListRequest;
import flyteidl.admin.ProjectOuterClass.ProjectRegisterRequest;
import flyteidl.admin.ProjectOuterClass.ProjectRegisterResponse;
import flyteidl.admin.ProjectOuterClass.Projects;
import flyteidl.admin.TaskOuterClass.TaskCreateRequest;
import flyteidl.admin.TaskOuterClass.TaskCreateResponse;
import flyteidl.admin.TaskOuterClass.TaskSpec;
import flyteidl.core.Execution.WorkflowExecution.Phase;
import flyteidl.core.IdentifierOuterClass.Identifier;
import flyteidl.core.IdentifierOuterClass.ResourceType;
import flyteidl.core.IdentifierOuterClass.WorkflowExecutionIdentifier;
import flyteidl.core.Interface.TypedInterface;
import flyteidl.core.Interface.VariableMap;
import flyteidl.core.Literals;
import flyteidl.core.Literals.Literal;
import flyteidl.core.Literals.LiteralMap;
import flyteidl.core.Literals.Primitive;
import flyteidl.core.Literals.RetryStrategy;
import flyteidl.core.Literals.Scalar;
import flyteidl.core.Tasks;
import flyteidl.core.Tasks.Container;
import flyteidl.core.Tasks.DataLoadingConfig;
import flyteidl.core.Tasks.RuntimeMetadata;
import flyteidl.core.Tasks.RuntimeMetadata.RuntimeType;
import flyteidl.core.Tasks.TaskMetadata;
import flyteidl.core.Tasks.TaskTemplate;
import flyteidl.service.AdminServiceGrpc;
import flyteidl.service.AdminServiceGrpc.AdminServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class FlyteProtoClient {

        private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
        private static final String MY_TASK_TYPE = "container";

        private static String CONTAINER_NAME = "docker.io/nginx:latest";
        private static String INPUT_PATH = "/tmp";
        private static String OUT_PATH = "/tmp";
        private static String SHELL_PATH = "sh";
        private static final String DOMAIN = "development";

        ManagedChannel channel;
        AdminServiceBlockingStub stub;

        FlyteProtoClient(String url) {
                URI uri = URI.create(url);
                System.out.println(uri.getHost() + " " + uri.getPort() + " " + uri.getScheme());
                this.channel = ManagedChannelBuilder.forTarget(uri.getHost() + ":" + uri.getPort()).usePlaintext()
                                .enableRetry().build();
                this.stub = AdminServiceGrpc.newBlockingStub(channel);
        }

        private static Literal asLiteral(Literals.Primitive primitive) {
                Scalar scalar = Scalar.newBuilder()
                                .setPrimitive(primitive)
                                .build();
                return Literal.newBuilder()
                                .setScalar(scalar)
                                .build();
        }

        public static Literals.Literal ofString(String value) {
                Primitive primitive = Primitive.newBuilder()
                                .setStringValue(value)
                                .build();
                return asLiteral(primitive);
        }

        public void waitForTheScriptToFinish(String project, String executionId, int maxTimeOutInSecs) {

                int waitTimeInSecs = 5;
                int loops = maxTimeOutInSecs / waitTimeInSecs;

                for (int i = 0; i < loops; i++) {
                        boolean status = this.checkExecutionRunning(project, executionId);
                        // If the task is completed break the loop.
                        if (status == false) {
                                logger.atInfo().log("Task completed successfully in the Flyte Console");
                                break;
                        }
                        Uninterruptibles.sleepUninterruptibly(java.time.Duration.ofSeconds(5));
                }
        }

        public void runShellScript(String shellScript, int maxTimeout) {

                String randomId = System.currentTimeMillis() + "" + (int) (Math.random() * 1000000);
                /// String project = String.format("project-%s", randomId);
                String project = "flytesnacks";
                String taskName = String.format("task-%s", randomId);
                // String version = String.format("v-%s", randomId);
                String version = "v1";

                // 1. Create a Project
                // boolean projectCreated = this.createProject(project);
                boolean projectCreated = true;

                if (projectCreated) {
                        // 2. Create a Task
                        boolean taskCreated = this.createTask(project, taskName, version, shellScript);

                        if (taskCreated) {
                                // 3. Run the task
                                String executionId = this.runTask(project, taskName, version);
                                if (executionId != null) {
                                        this.waitForTheScriptToFinish(project, executionId, maxTimeout);
                                } else {
                                        logger.atSevere().log(
                                                        "Unable to run task in Flyte Console with project: %s , task:%s, version:%s",
                                                        project, taskName, version);
                                }
                        } else {
                                logger.atSevere().log(
                                                "Unable to create task in Flyte Console with project: %s , task:%s, version:%s",
                                                project, taskName, version);
                        }

                } else {
                        logger.atSevere().log("Unable to create project in Flyte Console with name : %s", project);
                }

        }

        public boolean checkProjectExists(String project_id) {
                ProjectListRequest request = ProjectListRequest.newBuilder()
                                .build();
                try {
                        Projects projects = stub.listProjects(request);
                        for (int i = 0; i < projects.getProjectsCount(); i++) {
                                ProjectOuterClass.Project project = projects.getProjects(i);
                                if (project.getId().equals(project_id)) {
                                        return true;
                                }
                        }
                } catch (Exception e) {

                }
                return false;
        }

        public boolean createProject(String projectName) {

                Project project = Project.newBuilder()
                                .setId(projectName)
                                .setName(projectName)
                                // .addDomains(Domain.newBuilder().setName(DOMAIN).build())
                                .build();

                ProjectRegisterRequest request = ProjectRegisterRequest.newBuilder()
                                .setProject(project)
                                .build();

                ProjectRegisterResponse response = stub.registerProject(request);
                if (response != null) {
                        return this.checkProjectExists(projectName);
                }

                return false;
        }

        public boolean createTask(String project, String taskName, String version, String shellScript) {
                logger.atFine().log("Creating task  with project=%s, task=%s, version=%s, shellScript=%s", project,
                                taskName, version, shellScript);

                Identifier taskId = Identifier.newBuilder()
                                .setResourceType(ResourceType.TASK)
                                .setDomain(DOMAIN)
                                .setProject(project)
                                .setName(taskName)
                                .setVersion(version)
                                .build();

                TypedInterface taskInterface = TypedInterface.newBuilder()
                                .setInputs(VariableMap.newBuilder().build())
                                .setOutputs(VariableMap.newBuilder().build())
                                .build();

                RetryStrategy RETRIES = RetryStrategy.newBuilder()
                                .setRetries(1)
                                .build();

                // Spawan a docker image in the cluser and run the shell script
                Container container = Container.newBuilder()
                                .setImage(CONTAINER_NAME)
                                .setDataConfig(DataLoadingConfig.newBuilder()
                                                .setInputPath(INPUT_PATH)
                                                .setOutputPath(OUT_PATH).build())
                                .addAllArgs(Arrays.asList("-c", shellScript))
                                .addCommand(SHELL_PATH)
                                .build();

                RuntimeMetadata runMetadata = RuntimeMetadata.newBuilder()
                                .setType(RuntimeType.FLYTE_SDK)
                                .setVersion("0.0.1")
                                .setFlavor("java")
                                .build();

                TaskMetadata metadata = TaskMetadata.newBuilder()
                                .setDiscoverable(false)
                                .setCacheSerializable(false)
                                .setTimeout(Duration.newBuilder().setSeconds(180).build())
                                .setRetries(RETRIES)
                                .setRuntime(runMetadata)
                                .build();

                Tasks.TaskTemplate taskTemplate = TaskTemplate.newBuilder()
                                .setType(MY_TASK_TYPE)
                                .setInterface(taskInterface)
                                .setContainer(container)
                                .setMetadata(metadata)
                                .setCustom(Struct.newBuilder().build())
                                .build();

                TaskCreateRequest request = TaskCreateRequest.newBuilder()
                                .setId(taskId)
                                .setSpec(TaskSpec.newBuilder()
                                                .setTemplate(taskTemplate)
                                                .build())
                                .build();

                TaskCreateResponse response = stub.createTask(request);
                if (response != null) {
                        return true;
                }
                return false;
        }

        @SuppressWarnings("deprecation")
        public String runTask(String project, String taskName, String version) {

                Identifier launchId = Identifier.newBuilder()
                                .setResourceType(ResourceType.TASK)
                                .setProject(project)
                                .setDomain(DOMAIN)
                                .setName(taskName)
                                .setVersion(version)
                                .build();

                ExecutionMetadata metadata = ExecutionMetadata.newBuilder()
                                .setMode(ExecutionMode.MANUAL)
                                .setPrincipal("flyteconsole")
                                .build();

                ExecutionSpec executionSpec = ExecutionSpec.newBuilder()
                                .setMetadata(metadata)
                                .setLaunchPlan(launchId)
                                .setInputs(LiteralMap.newBuilder().build())
                                .build();

                ExecutionCreateRequest request = ExecutionCreateRequest.newBuilder()
                                .setDomain(DOMAIN)
                                .setProject(project)
                                .setSpec(executionSpec)
                                .build();
                ExecutionCreateResponse response = stub.createExecution(request);
                if (response != null) {
                        return response.getId().getName();
                }
                return null;
        }

        private boolean isRunning(Phase phase) {
                switch (phase) {
                        case SUCCEEDING:
                        case QUEUED:
                        case RUNNING:
                        case UNDEFINED:
                                return true;
                        case TIMED_OUT:
                        case SUCCEEDED:
                        case ABORTED:
                        case ABORTING:
                        case FAILED:
                        case FAILING:
                        case UNRECOGNIZED:
                                return false;
                }

                return false;
        }

        public boolean checkExecutionRunning(String project, String executionId) {

                WorkflowExecutionGetRequest request = WorkflowExecutionGetRequest.newBuilder()
                                .setId(WorkflowExecutionIdentifier.newBuilder()
                                                .setName(executionId)
                                                .setDomain(DOMAIN)
                                                .setProject(project).build())
                                .build();

                Execution resp = stub.getExecution(request);
                if (resp != null) {
                        return this.isRunning(resp.getClosure().getPhase());
                }

                return false;
        }

}