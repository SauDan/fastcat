import * as cdk from 'aws-cdk-lib';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';


const fargate_platform_version = ecs.FargatePlatformVersion.VERSION1_4;

const AL2023_IMAGE = ecs.ContainerImage.fromRegistry("public.ecr.aws/amazonlinux/amazonlinux:2023");

interface ECRRepoInfo {
    name: string,
    tag: string,
};

export interface BatchStackProps extends cdk.NestedStackProps {
    vpc: ec2.IVpc,
    image: ECRRepoInfo,
    nodejs_image: ECRRepoInfo,
    s3_configs: {
        in_bucket: s3.IBucket,  in_prefixes: string[],
        job_bucket: s3.IBucket, job_prefixes: string[],
        out_bucket: s3.IBucket, out_prefixes: string[],
    },
};

interface MetadataParsingJobProps extends BatchStackProps {
    concat_job: batch.IJobDefinition,
    metadata_generation_job: batch.IJobDefinition,
};

export class BatchStack extends cdk.NestedStack {
    queue: batch.IJobQueue;
    metadata_parsing_job: batch.IJobDefinition;

    private sg: ec2.ISecurityGroup;
    private job_role: iam.IRole;


    constructor(scope: Construct, id: string, props: BatchStackProps) {
        super(scope, id, props);

        const { node_path, addr8 } = this.get_node_info(scope);

        this.sg = new ec2.SecurityGroup(this, 'sg', {
            vpc: props.vpc,
            securityGroupName: `${node_path}-sg-${addr8}`,
        });

        this.job_role = this.make_job_role(scope, props);

        this.queue = new batch.JobQueue(this, 'queue', {
            computeEnvironments: this.make_compute_envs(scope, props.vpc),
            jobQueueName: `${node_path}-queue-${addr8}`,
        });

        const concat_job = this.make_concat_job(scope, props);
        concat_job.grantSubmitJob(this.job_role, this.queue);

        const metadata_generation_job =
            this.make_metadata_generation_job(scope, props);
        metadata_generation_job.grantSubmitJob(this.job_role, this.queue);

        this.metadata_parsing_job = this.make_metadata_parsing_job(scope, {
                ...props,
            concat_job,
            metadata_generation_job,
        });
    }


    get_node_info(self: Construct) {
        return {
            node_path: self.node.path.replace(/\//g, '-'),
            addr8: self.node.addr.substring(0, 8),
        };
    }


    private make_concat_job(scope: Construct, props: BatchStackProps) {
        const job = new Construct(this, 'concat');
        const { node_path, addr8 } = this.get_node_info(job);

        const image = ecs.ContainerImage.fromEcrRepository(
            ecr.Repository.fromRepositoryName(job, 'repo', props.image.name),
            props.image.tag);

        const job_def = new batch.EcsJobDefinition(job, 'job-def', {
            jobDefinitionName: `${node_path}-jobdef-${addr8}`,
            propagateTags: true,
            retryAttempts: 5,
            retryStrategies: [
                batch.RetryStrategy.of(batch.Action.EXIT,
                                       batch.Reason.CANNOT_PULL_CONTAINER),
            ],
            timeout: cdk.Duration.hours(4),
            container:  new batch.EcsFargateContainerDefinition(this, 'container-def', {
                cpu: 2,
                memory: cdk.Size.gibibytes(4),
                image: image,
                fargatePlatformVersion: fargate_platform_version,
                jobRole: this.job_role,
                command: [
                    "concat-fastq",
                    "Ref::job_file_s3_url",
                ],
                environment: {
                        ... BatchStack.common_environments(props),
                }
            }),
        });

        return job_def;
    }


    private static common_environments(props: BatchStackProps) {
        const s3 = props.s3_configs;
        return {
            FASTCAT_S3URL_OUTPUT_PREFIX:
                s3.out_bucket.s3UrlForObject(s3.out_prefixes[0]),
            FASTCAT_S3URL_JOBS_PREFIX:
                s3.job_bucket.s3UrlForObject(s3.job_prefixes[0]),
        };
    }


    private make_metadata_parsing_job(scope: Construct,
                                      props: MetadataParsingJobProps) {
        return this.make_nodejs_job(scope, props, 'metadata-parsing', [
            "process-metadata",
            "Ref::fastqlist_file_s3_url",
        ], {
            FASTCAT_JOB_QUEUE_ARN: this.queue.jobQueueArn,
            FASTCAT_CONCAT_JOB_DEF_ARN: props.concat_job.jobDefinitionArn,
            FASTCAT_METADATA_GENERATION_JOB_DEF_ARN: props.metadata_generation_job.jobDefinitionArn,
        });
    }

    private make_metadata_generation_job(scope: Construct,
                                         props: BatchStackProps) {
        return this.make_nodejs_job(scope, props, 'metadata-generation', [
            "consolidate-metadata",
            "Ref::job_file_s3_url",
        ]);
    }

    private make_nodejs_job(scope: Construct,
                            props: BatchStackProps,
                            name: string,
                            command: string[],
                            environment?: Record<string,string>) {
        const job = new Construct(this, name);
        const { node_path, addr8 } = this.get_node_info(job);

        const image = ecs.ContainerImage.fromEcrRepository(
            ecr.Repository.fromRepositoryName(job, 'repo', props.nodejs_image.name),
            props.nodejs_image.tag);

        const job_def = new batch.EcsJobDefinition(job, 'job-def', {
            jobDefinitionName: `${node_path}-jobdef-${addr8}`,
            propagateTags: true,
            retryAttempts: 5,
            retryStrategies: [
                batch.RetryStrategy.of(batch.Action.EXIT,
                                       batch.Reason.CANNOT_PULL_CONTAINER),
            ],
            timeout: cdk.Duration.minutes(10),
            container:  new batch.EcsFargateContainerDefinition(job, 'container-def', {
                cpu: 1,
                memory: cdk.Size.gibibytes(2),
                image: image,
                fargatePlatformVersion: fargate_platform_version,
                jobRole: this.job_role,
                command,
                environment: {
                        ...BatchStack.common_environments(props),
                        ...environment,
                },
            }),
        });
        return job_def;
    }


    private make_compute_envs(scope: Construct, vpc: ec2.IVpc) {
        const parent_path = scope.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);

        const spot = new batch.FargateComputeEnvironment(this, 'spot', {
            computeEnvironmentName: `${parent_path}-spot-${addr8}`,
            vpc,
            spot: true,
            securityGroups: [ this.sg ],
        });

        const ondemand = new batch.FargateComputeEnvironment(this, 'ondemand', {
            computeEnvironmentName: `${parent_path}-ondemand-${addr8}`,
            vpc,
            spot: false,
            securityGroups: [ this.sg ],
        });

        return [
            { computeEnvironment: spot,     order:1 },
            { computeEnvironment: ondemand, order:2 },
        ];
    }


    private make_job_role(scope: Construct, props: BatchStackProps) {
        const { node_path } = this.get_node_info(scope);
        const policy =
            new iam.ManagedPolicy(scope, 's3-access', {
                managedPolicyName: `${node_path}-s3Access`,
            });

        props.s3_configs.in_prefixes
            .map(p => props.s3_configs.in_bucket
                          .grantRead(policy, p + "/*"));

        props.s3_configs.job_prefixes
            .map(p => props.s3_configs.job_bucket
                          .grantReadWrite(policy, p + "/*"));

        props.s3_configs.out_prefixes
            .map(p => props.s3_configs.out_bucket
                          .grantWrite(policy, p + "/*"));


       const role = new iam.Role(scope, 'job-role', {
            roleName: `${node_path}-jobRole`,
            assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managedPolicies: [
                policy
            ],
        });

        return role;
    }
}
