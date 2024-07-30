import * as cdk from 'aws-cdk-lib';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as iam from 'aws-cdk-lib/aws-iam';
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
    metadata_parsing_image: ECRRepoInfo,
    s3_configs: {
        in_bucket: string,  in_prefixes: string[],
        out_bucket: string, out_prefixes: string[],
    },
};

export class BatchStack extends cdk.NestedStack {

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

        const queue = new batch.JobQueue(this, 'queue', {
            computeEnvironments: this.make_compute_envs(scope, props.vpc),
            jobQueueName: `${node_path}-queue-${addr8}`,
        });

        this.make_job(scope, props);
        this.make_metadata_parsing_job(scope, props);
    }


    get_node_info(self: Construct) {
        return {
            node_path: self.node.path.replace(/\//g, '-'),
            addr8: self.node.addr.substring(0, 8),
        };
    }


    make_job(scope: Construct, props: BatchStackProps) {
        const { node_path, addr8 } = this.get_node_info(scope);

        const image = ecs.ContainerImage.fromEcrRepository(
            ecr.Repository.fromRepositoryName(this, 'repo', props.image.name),
            props.image.tag);

        const job_def = new batch.EcsJobDefinition(this, 'job-def', {
            jobDefinitionName: `${node_path}-jobdef-${addr8}`,
            container:  new batch.EcsFargateContainerDefinition(this, 'container-def', {
                cpu: 2,
                memory: cdk.Size.gibibytes(4),
                image: image,
                fargatePlatformVersion: fargate_platform_version,
                jobRole: this.job_role,
                command: [
                    "/usr/bin/env",
                ],
            }),
        });
    }


    make_metadata_parsing_job(scope: Construct, props: BatchStackProps) {
        const job = new Construct(this, 'metadata-parsing');
        const { node_path, addr8 } = this.get_node_info(job);

        const image = ecs.ContainerImage.fromEcrRepository(
            ecr.Repository.fromRepositoryName(job, 'repo', props.metadata_parsing_image.name),
            props.metadata_parsing_image.tag);

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
                command: [
                    "process-metadata",
                    "Ref::metadata_file_s3_url",
                    "Ref::output_dir_s3_url",
                ],
            }),
        });
    }


    make_compute_envs(scope: Construct, vpc: ec2.IVpc) {
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


    make_job_role(scope: Construct, props: BatchStackProps) {
        const { node_path } = this.get_node_info(scope);
        const policy =
            new iam.ManagedPolicy(scope, 's3-access', {
                managedPolicyName: `${node_path}-s3Access`,
                statements: [
                    new iam.PolicyStatement({
                        actions: [ "s3:GetObject" ],
                        resources: props.s3_configs.in_prefixes.map(p =>
                            `arn:aws:s3:::${props.s3_configs.in_bucket}/${p}/*` //*/
                                                                   ),
                    }),
                    new iam.PolicyStatement({
                        actions: [ "s3:PutObject" ],
                        resources: props.s3_configs.out_prefixes.map(p =>
                            `arn:aws:s3:::${props.s3_configs.out_bucket}/${p}/*` //*/
                                                                    ),
                    }),
                ],
            });

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
