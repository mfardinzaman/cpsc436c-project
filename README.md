# Translytics: TransLink Analytics

Deployed with AWS Amplify: https://main.d3mj2q6l6unypg.amplifyapp.com/

Public transit reliability often frustrates commuters due to inaccurate scheduling. Currently, insights based on historical data are severely lacking in existing solutions, such as the Transit app. This project addresses these challenges by developing a web application that delivers analytics on TransLink bus performance through an intuitive dashboard. Users can assess metrics like average delays, lateness patterns, and route statistics, enabling informed route planning.

## Architecture
<img src ="images/architecture.jpg">

### AWS Services Used
- Eventbridge
- Lambda
- S3
- Keyspaces
- ECR
- Amplify
- API Gateway

## Setup & Starting the application locally
 - Install Node (v18.11.18, but any v18 should work)
 - run `npm install` in the root directory and `npm install --legacy-peer-deps` in the frontend directory
 - run `npm start` 
