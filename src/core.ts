import {
    _Object,
    CopyObjectCommand,
    DeleteObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsCommand,
    PutObjectCommand,
    PutObjectCommandInput,
} from "@aws-sdk/client-s3";
import { AWS_BUCKET, s3client } from "./aws";
import { Readable } from "stream";
import { cache } from "react";
import { z } from "zod";
import { InterParams, SupportedLanguage } from "./internationalization";
import { objIsHidden } from "@/utils/utils";

// Apparently the stream parameter should be of type Readable|ReadableStream|Blob
// The latter 2 don't seem to exist anywhere.
async function streamToString(stream: Readable): Promise<string> {
    return await new Promise((resolve, reject) => {
        const chunks: Uint8Array[] = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    });
}

const s3Key = (objKey: string, lang: SupportedLanguage) => (lang === "en" ? objKey : lang + "/" + objKey);

interface FetchObjectOptions {
    filter?: (value: _Object, index: number, array: _Object[]) => boolean;
    showHidden?: boolean;
    interParams: InterParams | null;
}

/**
 * // TODO load range of objects
 */
export const fetchObjects = cache(async (options: FetchObjectOptions) => {
    const command = new ListObjectsCommand({
        Bucket: AWS_BUCKET,
        MaxKeys: 20,
    });
    const s3response = await s3client.send(command);
    const contents = s3response.Contents || [];
    const filtered = contents.filter((obj) => {
        if (!obj.Key) return false;
        // Apply language filter
        if (
            options.interParams?.lang &&
            options.interParams.lang !== "en" &&
            !obj.Key.startsWith(options.interParams!.lang + "/")
        )
            return false;
        // Apply custom filter
        if (options.filter && !options.filter(obj, 0, contents)) return false;
        // Hidden files are those that end with a $ or $.<file_extension>
        if (!options.showHidden && objIsHidden(obj.Key)) return false;
        return true;
    });

    return filtered;
});

export const fetchObject = cache(async (key: string) => {
    const command = new GetObjectCommand({
        Bucket: AWS_BUCKET,
        Key: key,
    });
    const s3response = await s3client.send(command);

    return s3response;
});

export const fetchObjectMetadata = cache(async (key: string) => {
    const command = new HeadObjectCommand({
        Bucket: AWS_BUCKET,
        Key: key,
    });
    const s3response = await s3client.send(command);

    return s3response;
});

export const fetchObjectText = cache(async (key: string) => {
    const command = new GetObjectCommand({
        Bucket: AWS_BUCKET,
        Key: key,
    });
    const s3response = await s3client.send(command);

    if (!s3response.Body) return "";

    return await streamToString(s3response.Body as Readable);
});

export function deleteObject(key: string) {
    const command = new DeleteObjectCommand({
        Bucket: AWS_BUCKET,
        Key: key,
    });
    return s3client.send(command);
}

export async function putObject(
    key: string,
    data: string | Buffer | Uint8Array | Readable,
    input?: Partial<PutObjectCommandInput>
) {
    if (key.startsWith("/")) throw new Error("Key cannot start with '/'");
    const command = new PutObjectCommand({
        ...input,
        Bucket: AWS_BUCKET,
        Key: key,
        Body: data,
        Metadata: parseMetadata({
            // force last order
            $order: new Date().getTime(),
            $show: false,
        } as CustomObjMetadata),
    });
    return s3client.send(command);
}

const CustomObjMetadataSchema = z.object({
    $order: z.number().optional(),
});

export type CustomObjMetadata = z.infer<typeof CustomObjMetadataSchema>;

function parseMetadata(metadata: unknown): Record<string, string> {
    const parsedMetadata = CustomObjMetadataSchema.optional().parse(metadata);
    return Object.entries(parsedMetadata || {}).reduce((acc, [key, value]) => {
        if (value === undefined) return acc;
        acc[key] = value.toString();
        return acc;
    }, {} as Record<string, string>);
}

/**
 * Rename an o bject or update it's metadata.
 */
export async function updateObject(oldKey: string, newKey: string, newMetadata?: Partial<CustomObjMetadata>) {
    /*
    AWS S3 object metadata cannot be mofified directly, so we need to copy the object to a new key
    */

    if (!newKey) throw new Error("newKey is required");

    const metadata = await fetchObjectMetadata(oldKey);
    const mergedNewMetadata = { ...metadata.Metadata, ...parseMetadata(newMetadata) };
    const cmd = new CopyObjectCommand({
        Bucket: AWS_BUCKET,
        CopySource: `${AWS_BUCKET}/${oldKey}`,
        // rename
        Key: newKey,
        // modify metadata
        Metadata: mergedNewMetadata,
        MetadataDirective: "REPLACE",
    });
    await s3client.send(cmd);
    if (oldKey !== newKey) await deleteObject(oldKey);
}
