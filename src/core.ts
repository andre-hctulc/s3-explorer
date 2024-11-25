import {
    _Object,
    CopyObjectCommand,
    CopyObjectCommandInput,
    DeleteObjectCommand,
    GetObjectCommand,
    GetObjectCommandInput,
    GetObjectCommandOutput,
    HeadObjectCommand,
    HeadObjectCommandInput,
    ListObjectsCommand,
    ListObjectsCommandInput,
    PutObjectCommand,
    PutObjectCommandInput,
    S3Client,
    S3ClientConfig,
} from "@aws-sdk/client-s3";
import type { Readable } from "stream";
import type { Command } from "@smithy/smithy-client";

async function streamToString(stream: Readable): Promise<string> {
    return await new Promise((resolve, reject) => {
        const chunks: Uint8Array[] = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    });
}

function parseMetadata(metadata: Record<string, any>): Record<string, string> {
    if (!metadata || typeof metadata !== "object") return {};
    return Object.entries(metadata || {}).reduce((acc, [key, value]) => {
        if (value === undefined) return acc;
        acc[key] = value.toString();
        return acc;
    }, {} as Record<string, string>);
}

interface FetchHeadsOptions {
    filter?: (value: _Object, index: number, array: _Object[]) => boolean;
}

type GetObjectCommandOutputBody = Exclude<GetObjectCommandOutput["Body"], undefined> | null;

type S3ConnectionConfig<M extends object = Record<string, string>> = {
    client: S3ClientConfig | S3Client;
    mergeMetadata?: (m1: Partial<M>, m2: Partial<M>) => M;
};

type AnyCommand = Command<any, any, any, any>;

/**
 * @template M Metadata
 */
export class BucketConnection<M extends object = Record<string, string>> {
    readonly bucketName: string;
    readonly client: S3Client;
    private _config: S3ConnectionConfig<M>;

    constructor(bucketName: string, config: S3ConnectionConfig<M>) {
        this.bucketName = bucketName;
        this._config = config;
        this.client = config.client instanceof S3Client ? config.client : new S3Client(config.client);
    }

    send(command: AnyCommand) {
        return this.client.send(command);
    }

    /**
     * Creates a URL for this bucket with optional key.
     * @example `https://example-bucket.s3.amazonaws.com/optional/path/to/object`
     */
    url() {
        return BucketConnection.url(this.bucketName);
    }

    /**
     * Creates a URL for a bucket with optional key.
     * @example `https://example-bucket.s3.amazonaws.com/optional/path/to/object`
     */
    static url(bucketName: string, options?: { key?: string; protocol?: string }) {
        let uri = `${options?.protocol || "https:"}//${bucketName}.s3.amazonaws.com`;
        if (options?.key) uri += `/${options.key}`;
        return uri;
    }

    /**
     * Parses a URL to get the bucket name and key.
     */
    static parseUrl(url: string): { bucketName: string; key?: string } | null {
        const regex = /^.+:\/\/([^\/]+)\.s3\.amazonaws\.com(\/.+)?$/;
        const match = url.match(regex);

        if (!match) return null;

        const bucketName = match[1];
        const key = match[2]?.substring(1) || undefined;

        return { bucketName, key };
    }

    // -- Objects

    getCommand(key: string, input?: Partial<GetObjectCommandInput>): GetObjectCommand {
        return new GetObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
    }

    async get(key: string): Promise<GetObjectCommandOutputBody> {
        const command = await this.getCommand(key);
        const s3response = await this.client.send(command);
        return s3response.Body || null;
    }

    async getText(key: string) {
        const command = await this.getCommand(key);
        const s3response = await this.client.send(command);
        if (!s3response.Body) return "";
        return await streamToString(s3response.Body as Readable);
    }

    async putCommand(
        key: string,
        data: string | Buffer | Uint8Array | Readable | Blob,
        input?: Partial<PutObjectCommandInput>
    ): Promise<PutObjectCommand> {
        let contentType: string | undefined;

        // Blob is not support in node env, so we convert it to Buffer
        if (data instanceof Blob && typeof window === "undefined") {
            contentType = data.type;
            data = Buffer.from(await data.arrayBuffer());
        }

        return new PutObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            Body: data,
            ContentType: contentType,
            ...input,
        });
    }

    /**
     * @param data Blobs are converted to Buffers.
     */
    async put(key: string, data: string | Buffer | Uint8Array | Readable | Blob): Promise<void> {
        const command = await this.putCommand(key, data);
        await this.client.send(command);
    }

    deleteCommand(key: string, input?: Partial<DeleteObjectCommand>) {
        return new DeleteObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
    }

    async del(key: string): Promise<void> {
        const command = this.deleteCommand(key);
        await this.client.send(command);
    }

    copyCommand(oldKey: string, newKey: string, input?: Partial<CopyObjectCommandInput>): CopyObjectCommand {
        return new CopyObjectCommand({
            Bucket: this.bucketName,
            CopySource: `${this.bucketName}/${oldKey}`,
            // rename
            Key: newKey,
            ...input,
        });
    }

    async rename(oldKey: string, newKey: string): Promise<void> {
        if (oldKey === newKey) return;
        const copyCommand = this.copyCommand(oldKey, newKey);
        await this.client.send(copyCommand);
        const delCommand = this.deleteCommand(oldKey);
        await this.client.send(delCommand);
    }

    // -- Head

    getHeadCommand(key: string, input?: Partial<HeadObjectCommandInput>): HeadObjectCommand {
        return new HeadObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
    }

    async getHead(key: string): Promise<Partial<M>> {
        const command = this.getHeadCommand(key);
        const s3response = await this.client.send(command);
        return (s3response.Metadata as Partial<M> | undefined) || {};
    }

    getHeadsCommand(input?: Partial<ListObjectsCommandInput>): ListObjectsCommand {
        return new ListObjectsCommand({
            Bucket: this.bucketName,
            MaxKeys: 20,
            ...input,
        });
    }

    async getHeads(options: FetchHeadsOptions) {
        const command = this.getHeadsCommand();
        const s3response = await this.client.send(command);
        const contents = s3response.Contents || [];
        const filtered = contents.filter((obj) => {
            if (!obj.Key) return false;
            // Apply custom filter
            if (options.filter && !options.filter(obj, 0, contents)) return false;
            return true;
        });

        return filtered;
    }

    async putHead(key: string, metadata: Partial<M>) {
        const currentMetadata = await this.getHead(key);
        const newMetadata = this._config.mergeMetadata
            ? this._config.mergeMetadata(currentMetadata, metadata)
            : metadata;
        await this.copyCommand(key, key, {
            MetadataDirective: "REPLACE",
            Metadata: parseMetadata(newMetadata),
        });
    }
}
