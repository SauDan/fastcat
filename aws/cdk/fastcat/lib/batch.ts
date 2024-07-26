import * as cdk from 'aws-cdk-lib';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import { Construct } from 'constructs';


const fargate_platform_version = ecs.FargatePlatformVersion.VERSION1_4;
const container_image_name = "public.ecr.aws/amazonlinux/amazonlinux:2023";


interface BatchStackProps extends cdk.NestedStackProps {
    vpc: ec2.IVpc,
};

export class BatchStack extends cdk.NestedStack {

    private sg: ec2.ISecurityGroup;


    constructor(scope: Construct, id: string, props: BatchStackProps) {
        super(scope, id, props);

        const parent_path = scope.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);

        this.sg = new ec2.SecurityGroup(this, 'sg', {
            vpc: props.vpc,
            securityGroupName: `${parent_path}-sg-${addr8}`,
        });

        const queue = new batch.JobQueue(this, 'queue', {
            computeEnvironments: this.make_compute_envs(scope, props.vpc),
            jobQueueName: `${parent_path}-queue-${addr8}`,
        });

        const job_def = new batch.EcsJobDefinition(this, 'job-def', {
            jobDefinitionName: `${parent_path}-jobdef-${addr8}`,
            container:  new batch.EcsFargateContainerDefinition(this, 'container-def', {
                cpu: 2,
                memory: cdk.Size.gibibytes(4),
                image: ecs.ContainerImage.fromRegistry(container_image_name),
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

        console.log(`${parent_path}-spot-${addr8}`);

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
