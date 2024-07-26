import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { NetworkStack } from './network';
import { BatchStack } from './batch';


export class FastcatStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const network_stack = new NetworkStack(this, 'network', props);
        const batch_stack = new BatchStack(this, 'batch', {
                ... props,
            vpc: network_stack.vpc,
        });
    }
}
