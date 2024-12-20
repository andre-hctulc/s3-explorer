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
import { Readable } from "stream";
import type { Command } from "@smithy/smithy-client";

interface FetchHeadsOptions {
    filter?: (value: _Object, index: number, array: _Object[]) => boolean;
}

type GetObjectCommandOutputBody = Exclude<GetObjectCommandOutput["Body"], undefined> | null;

type S3ConnectionConfig<M extends object = Record<string, string>> = {
    client: S3ClientConfig | S3Client;
    mergeMetadata?: (m1: Partial<M>, m2: Partial<M>) => M;
};

type AnyCommand = Command<any, any, any, any>;

interface UrlOptions {
    key?: string;
    protocol?: string;
    credentials?: {
        accessKey: string;
        secretKey: string;
    };
}

interface ParsedUrl {
    bucketName: string;
    key: string | null;
    accessKey: string | null;
    secretKey: string | null;
    protocol: string;
}

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

    static fromUrl<M extends object = Record<string, string>>(
        url: string,
        config?: Partial<S3ConnectionConfig<M>>
    ): BucketConnection<M> | null {
        const parsed = BucketConnection.parseUrl(url);

        if (!parsed) return null;

        return new BucketConnection(parsed.bucketName, {
            ...config,
            client:
                config?.client instanceof S3Client
                    ? config.client
                    : {
                          ...config?.client,
                          credentials:
                              parsed.accessKey && parsed.secretKey
                                  ? {
                                        accessKeyId: parsed.accessKey,
                                        secretAccessKey: parsed.secretKey,
                                        ...config?.client?.credentials,
                                    }
                                  : config?.client?.credentials,
                      },
        });
    }

    send(command: AnyCommand) {
        return this.client.send(command);
    }

    /**
     * Creates a URL for this bucket with optional key.
     * @example `https://<BUCKET_NAME>.s3.amazonaws.com/optional/path/to/object`
     */
    url(options?: UrlOptions) {
        return BucketConnection.url(this.bucketName, options);
    }

    /**
     * Creates a URL for a bucket with optional key.
     * @example `https://<ACCESS_KEY>:<SECRET_KEY>@<BUCKET_NAME>.s3.amazonaws.com/optional/path/to/object`
     */
    static url(bucketName: string, options?: UrlOptions) {
        let uri = options?.protocol || "https://";

        if (options?.credentials) {
            uri += `${options.credentials.accessKey}:${options.credentials.secretKey}@`;
        }

        uri += `${bucketName}.s3.amazonaws.com`;

        if (options?.key) {
            uri += `/${options.key}`;
        }

        return uri;
    }

    /**
     * Parses a URL to get the bucket name and key.
     */
    static parseUrl(url: string): ParsedUrl | null {
        const regex = /^(.+:\/\/)(?:([^:]+):([^@]+)@)?([^\/]+)\.s3\.amazonaws\.com(\/.*)?$/;
        const match = url.match(regex);

        if (!match) return null;

        const protocol = match[1];
        const accessKey = match[2] || null;
        const secretKey = match[3] || null;
        const bucketName = match[4];
        const key = match[5]?.substring(1) || null;

        return { accessKey, secretKey, bucketName, key, protocol };
    }

    // #### Objects ####

    getCommand(key: string, input?: Partial<GetObjectCommandInput>): GetObjectCommand {
        return new GetObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
    }

    /**
     * Use `S3Connection.streamTo*` utils to convert the body to the desired format.
     */
    async get(key: string): Promise<GetObjectCommandOutputBody> {
        const command = await this.getCommand(key);
        const s3response = await this.client.send(command);
        return s3response.Body || null;
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

    async delete(key: string): Promise<void> {
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

    async copy(oldKey: string, newKey: string): Promise<void> {
        const command = this.copyCommand(oldKey, newKey);
        await this.client.send(command);
    }

    async rename(oldKey: string, newKey: string): Promise<void> {
        if (oldKey === newKey) return;
        const copyCommand = this.copyCommand(oldKey, newKey);
        await this.client.send(copyCommand);
        const delCommand = this.deleteCommand(oldKey);
        await this.client.send(delCommand);
    }

    // #### Head ####

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

    listHeadsCommand(input?: Partial<ListObjectsCommandInput>): ListObjectsCommand {
        return new ListObjectsCommand({
            Bucket: this.bucketName,
            MaxKeys: 20,
            ...input,
        });
    }

    async listHeads(options: FetchHeadsOptions) {
        const command = this.listHeadsCommand();
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
            Metadata: this.parseMetadata(newMetadata),
        });
    }

    // #### Utils ####

    static async bodyToBuffer(body: GetObjectCommandOutputBody): Promise<Buffer> {
        if (body instanceof Readable) {
            return BucketConnection.streamToBuffer(body);
        } else if (body instanceof Buffer) {
            return Promise.resolve(body);
        } else if (body instanceof Uint8Array) {
            return Promise.resolve(Buffer.from(body));
        } else if (body instanceof Blob) {
            return Promise.resolve(Buffer.from(await body.arrayBuffer()));
        } else {
            return Promise.resolve(Buffer.from(""));
        }
    }

    static async bodyToString(body: GetObjectCommandOutputBody): Promise<string> {
        if (body instanceof Readable) {
            return BucketConnection.streamToString(body);
        } else if (body instanceof Buffer) {
            return Promise.resolve(body.toString("utf-8"));
        } else if (body instanceof Uint8Array) {
            return Promise.resolve(Buffer.from(body).toString("utf-8"));
        } else if (body instanceof Blob) {
            return Promise.resolve((await body.text()) || "");
        } else {
            return Promise.resolve("");
        }
    }

    static streamToString(stream: Readable): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            const chunks: Uint8Array[] = [];
            stream.on("data", (chunk) => chunks.push(chunk));
            stream.on("error", reject);
            stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
        });
    }

    static streamToBuffer(stream: Readable): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            const chunks: Uint8Array[] = [];
            stream.on("data", (chunk) => chunks.push(chunk));
            stream.on("error", reject);
            stream.on("end", () => resolve(Buffer.concat(chunks)));
        });
    }

    private parseMetadata(metadata: Record<string, any>): Record<string, string> {
        if (!metadata || typeof metadata !== "object") return {};

        return Object.entries(metadata || {}).reduce((acc, [key, value]) => {
            if (value === undefined) return acc;
            acc[key] = value.toString();
            return acc;
        }, {} as Record<string, string>);
    }
}
