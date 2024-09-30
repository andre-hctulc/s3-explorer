import { S3Connection } from "../src/core";

const conn = new S3Connection(process.env.BUCKET_NAME || "", {
    client: {
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
        },
        region: process.env.AWS_REGION,
    },
});
