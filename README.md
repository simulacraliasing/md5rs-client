# A Megadetector gRPC client

This is a simple gRPC client built on [tonic](https://github.com/hyperium/tonic) to interact with [md5rs-server](https://github.com/simulacraliasing/md5rs-server).
The client retains all features of the offline version [md5rs](https://github.com/simulacraliasing/md5rs).

## What are done by client side?

- Image and video decoding
- Frame preprocessing(resize)
- Encoding frame to webp and sending to server
- Receiving detection results from server
- Exporting detection results to json/csv file

## What are done by server side?

- Authentication
- Receiving webp encoded frames and decoding
- Frame preprocessing(padding)
- Inferencing
- Postprocessing including NMS
- Returning detection results to client

## Get started

### Install FFmpeg (for video processing)

You can download it on your platform from [here](https://ffmpeg.org/download.html). Place ffmpeg/ffmpeg.exe in the same folder as the md5rs-client/md5rs-client.exe .

### Run the client

To process a folder of cameratrap media and export result in csv format, you can use the following command:

Windows:

`md5rs.exe -f <folder_to_process> -t <access_token> -e csv`

Linux/MacOS:

`md5rs -f <folder_to_process> -t <access_token> -e csv`

The default grpc server backend is `https://md5rs.hinature.cn`, which is maintained by [Shanshui Conservation Center](http://www.shanshui.org). We are planning to make it an alternative to [红外相机照片AI识别助手]("https://cameratrap-ai.hinature.cn/home") and will provide access token generation in the future.

### (Optional) Organize media

check [Organize python script](https://github.com/simulacraliasing/md5rs?tab=readme-ov-file#organize-python-script)