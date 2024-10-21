use objectdetection::object_detection_client::ObjectDetectionClient;
use objectdetection::DetectRequest;
use tonic::Request;
use std::time::Instant;
use tonic::transport::Channel;
use uuid::Uuid;

pub mod objectdetection {
    tonic::include_proto!("objectdetection");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a channel to the server
    let channel = Channel::from_static("http://127.0.0.1:50051").connect().await?;

    // Create a client
    let mut client = ObjectDetectionClient::new(channel);

    // Number of requests to simulate
    let num_requests = 50;

    let outbound = async_stream::stream! {
        for _ in 0..num_requests {
            yield DetectRequest {
                uuid: Uuid::new_v4().to_string(),
                image: vec![0; 1024], // Dummy image data
            };
        }
    };

    // Start timing
    let start = Instant::now();

    let response = client.detect(Request::new(outbound)).await?;

    let mut inbound = response.into_inner();

    while let Some(response) = inbound.message().await? {
        println!("RESPONSE = {:?}", response);
    }

    // Stop timing
    let duration = start.elapsed();
    let avg_duration = duration / num_requests as u32;

    // Print total and average duration
    println!("Total time: {:?}", duration);
    println!("Average time per request: {:?}", avg_duration);

    Ok(())
}
