use std::pin::Pin;
use std::sync::Arc;
use std::thread;

use crossbeam::channel;
use tokio::sync::oneshot;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

use objectdetection::object_detection_server::{ObjectDetection, ObjectDetectionServer};
use objectdetection::{DetectRequest, DetectResponse, Object};

pub mod objectdetection {
    tonic::include_proto!("objectdetection");
}

struct InferenceTask {
    uuid: String,
    image_data: Vec<u8>,
    response_sender: oneshot::Sender<DetectResponse>,
}

#[derive(Debug)]
pub struct MyObjectDetection {
    sender: channel::Sender<InferenceTask>,
}

#[tonic::async_trait]
impl ObjectDetection for MyObjectDetection {
    type DetectStream =
        Pin<Box<dyn Stream<Item = Result<DetectResponse, Status>> + Send + 'static>>;

    async fn detect(
        &self,
        request: Request<tonic::Streaming<DetectRequest>>,
    ) -> Result<Response<Self::DetectStream>, Status> {
        let mut stream = request.into_inner();

        let sender = self.sender.clone();

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                match req {
                    Ok(detect_request) => {
                        let uuid = detect_request.uuid.clone();
                        let image_data = detect_request.image.clone();

                        let (task_response_sender, task_response_receiver) = oneshot::channel();

                        let task = InferenceTask {
                            uuid,
                            image_data,
                            response_sender: task_response_sender,
                        };

                        if sender.send(task).is_err() {
                            eprintln!("Failed to send inference task");
                            continue;
                        }

                        let response_tx = response_tx.clone();
                        tokio::spawn(async move {
                            if let Ok(response) = task_response_receiver.await {
                                let _ = response_tx.send(Ok(response)).await;
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Error receiving request: {:?}", e);
                        break;
                    }
                }
            }
        });

        let response_stream = ReceiverStream::new(response_rx);

        Ok(Response::new(
            Box::pin(response_stream) as Self::DetectStream
        ))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = channel::unbounded::<InferenceTask>();
    let receiver = Arc::new(receiver);

    // Create worker threads
    let num_workers = 64;
    for _ in 0..num_workers {
        let receiver = Arc::clone(&receiver);
        thread::spawn(move || {
            while let Ok(task) = receiver.recv() {
                // Perform inference
                let result = perform_inference(task.uuid, task.image_data);

                // Send the result back
                let _ = task.response_sender.send(result);
            }
        });
    }

    // Start the gRPC server
    let addr = "127.0.0.1:50051".parse()?;
    let object_detection = MyObjectDetection { sender };

    println!("ObjectDetectionServer listening on {}", addr);

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async {
            Server::builder()
                .add_service(ObjectDetectionServer::new(object_detection))
                .serve(addr)
                .await
        })?;

    Ok(())
}

// Dummy inference function
fn perform_inference(uuid: String, _image_data: Vec<u8>) -> DetectResponse {
    // Replace with actual inference logic
    let objects = vec![Object {
        name: "dummy_object".to_string(),
        confidence: 0.9,
        bounding_box: vec![0.1, 0.1, 0.2, 0.2],
    }];
    thread::sleep(std::time::Duration::from_secs(1)); // Simulate long inference time
    DetectResponse { uuid, objects }
}
