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
};

export class BatchStack extends cdk.NestedStack {

    private sg: ec2.ISecurityGroup;


    constructor(scope: Construct, id: string, props: BatchStackProps) {
        super(scope, id, props);

        const { node_path, addr8 } = this.get_node_info(scope);

        this.sg = new ec2.SecurityGroup(this, 'sg', {
            vpc: props.vpc,
            securityGroupName: `${node_path}-sg-${addr8}`,
        });

        const queue = new batch.JobQueue(this, 'queue', {
            computeEnvironments: this.make_compute_envs(scope, props.vpc),
            jobQueueName: `${node_path}-queue-${addr8}`,
        });

        this.make_job(scope, props);
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
                command: [
                    "/usr/bin/env",
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
}
