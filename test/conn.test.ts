// TODO

// import { S3Connection } from "../src/core";

// const conn = new S3Connection(process.env.BUCKET_NAME || "", {
//     client: {
//         credentials: {
//             accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
//             secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
//         },
//         region: process.env.AWS_REGION,
//     },
// });

// function entries(): Promise<string[]> {
//     return [];
// }

// function compareEntries(entries: string[], expected: string[]) {
//     return { missing: [], extra: [], equal: true };
// }

// describe("S3Connection", () => {
//     it("should connect to S3", async () => {
//         const data = await conn.getText("test.txt");
//         expect(data).toBe("Hello, world!");
//     });

//     it("should put data to S3", async () => {
//         await conn.put("test.txt", "Hello, world!");
//         const data = await conn.getText("test.txt");
//         expect(data).toBe("Hello, world!");
//     });
// });
