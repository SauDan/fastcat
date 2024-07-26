import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

export class NetworkStack extends cdk.NestedStack {
    vpc: ec2.IVpc;
    
    constructor(scope: Construct, id: string, props?: cdk.NestedStackProps) {
        super(scope, id, props);

        this.vpc = ec2.Vpc.fromLookup(this, 'vpc', {
            vpcId: 'vpc-22e50e4b',
        });

        /*
        console.dir({
            pub: vpc.publicSubnets,
            priv: vpc.privateSubnets,
            iso: vpc.isolatedSubnets,
            });
        */
    }
}
