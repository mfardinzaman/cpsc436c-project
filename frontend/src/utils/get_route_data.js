import AWS from 'aws-sdk'

// require('dotenv').config()

const invokeLambda = (lambda, params) => new Promise((resolve, reject) => {
    lambda.invoke(params, (error, data) => {
      if (error) {
        reject(error);
      } else {
        resolve(data);
      }
    });
});

export const retrieveRoutes = () => {
    AWS.config.update({ 
        accessKeyId: process.env.REACT_APP_AWS_ACCESS_KEY_ID, 
        secretAccessKey: process.env.REACT_APP_AWS_SECRET_ACCESS_KEY,
        sessionToken: process.env.REACT_APP_AWS_SESSION_TOKEN,
        region: 'ca-central-1',
    });
    
    const lambda = new AWS.Lambda();
    
    const params = {
        FunctionName: 'retrieveRoutes', 
        Payload: JSON.stringify({}),
    };
    
    return invokeLambda(lambda, params);
}