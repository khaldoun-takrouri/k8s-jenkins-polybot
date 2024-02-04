 pipeline {
    agent any
    environment {
        ECR_REGISTRY = "933060838752.dkr.ecr.eu-west-1.amazonaws.com"
        IMAGE_TAG = "${env.BUILD_NUMBER}"
        ECR_REGION = "eu-west-1"
        AWS_CREDENTIALS_ID = 'AWS credentials'
        KUBE_CONFIG_CRED = 'KUBE_CONFIG_CRED'
        CLUSTER_NAME = "k8s-main"
        CLUSTER_REGION = "us-east-1"
    }
    stages {
        stage('Login to AWS ECR') {
            steps {
                script {
                    withCredentials([aws(credentialsId: AWS_CREDENTIALS_ID, accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                        sh 'aws ecr get-login-password --region ${ECR_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}'
                    }
                }
            }
        }
        stage('Build and Push') {
            steps {
                script {
                    echo "IMAGE_TAG: ${IMAGE_TAG}"
                    sh 'docker build -t khaldoun-polybot .'
                    sh 'docker tag khaldoun-polybot ${ECR_REGISTRY}/khaldoun-polybot:${IMAGE_TAG}'
                    sh 'docker push ${ECR_REGISTRY}/khaldoun-polybot:${IMAGE_TAG}'
                }
            }
        }
        stage('Deploy') {
            steps {
                script {
                    withCredentials([aws(credentialsId: AWS_CREDENTIALS_ID, accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                        sh 'aws eks update-kubeconfig --region ${CLUSTER_REGION} --name ${CLUSTER_NAME}'
                        withCredentials([file(credentialsId: 'KUBE_CONFIG_CRED', variable: 'KUBECONFIG')]) {
                            sh "sed -i 's|image: .*|image: ${ECR_REGISTRY}/khaldoun-polybot:${IMAGE_TAG}|' khaldoun-masad.yaml"
                            sh 'kubectl apply -f khaldoun-masad.yaml' //--validate=false'
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            sh 'docker rmi $(docker images -q) -f'
        }
    }
}













