#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { FastcatStack } from '../lib/fastcat-stack';

const app = new cdk.App();
const top = new cdk.Stage(app, 'fastcat');
const stage = new cdk.Stage(top, process.env.STAGE || 'dev');

new FastcatStack(stage, 'Stack');


const tags = cdk.Tags.of(stage);
tags.add('projectName', 'fastcat');
tags.add('Owner', 'Sau Dan LEE');
tags.add('Application', top.node.id);
tags.add('Stage', stage.node.id);


//end
