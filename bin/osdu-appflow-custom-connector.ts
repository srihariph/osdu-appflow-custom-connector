#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { OsduAppflowCustomConnectorStack } from '../lib/osdu-appflow-custom-connector-stack';

const app = new cdk.App();
new OsduAppflowCustomConnectorStack(app, 'OsduAppflowCustomConnectorStack', {
});