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
    HeadObjectCommandOutput,
    ListObjectsCommand,
    ListObjectsCommandInput,
    ListObjectsCommandOutput,
    PutObjectCommand,
    PutObjectCommandInput,
    S3Client,
    S3ClientConfig,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";

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

interface FetchHeadesOptions {
    filter?: (value: _Object, index: number, array: _Object[]) => boolean;
}

type GetObjectCommandOutputBody = Exclude<GetObjectCommandOutput["Body"], undefined> | null;

type S3ConnectionConfig<M extends object = Record<string, string>> = {
    client: S3ClientConfig | S3Client;
    mergeMetadata?: (m1: Partial<M>, m2: Partial<M>) => M;
};

/**
 * @template M Metadata
 */
export class S3Connection<M extends object = Record<string, string>> {
    readonly bucketName: string;
    private _client: S3Client;
    private _config: S3ConnectionConfig<M>;

    constructor(bucketName: string, config: S3ConnectionConfig<M>) {
        this.bucketName = bucketName;
        this._config = config;
        this._client = config.client instanceof S3Client ? config.client : new S3Client(config.client);
    }

    // -- Objects

    async getRaw(key: string, input?: Partial<GetObjectCommandInput>): Promise<GetObjectCommandOutput> {
        const command = new GetObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
        const s3response = await this._client.send(command);
        return s3response;
    }

    async get(key: string): Promise<GetObjectCommandOutputBody> {
        const s3response = await this.getRaw(key);
        return s3response.Body || null;
    }

    async getText(key: string) {
        const s3response = await this.getRaw(key);
        if (!s3response.Body) return "";
        return await streamToString(s3response.Body as Readable);
    }

    async putRaw(key: string, input?: Partial<PutObjectCommandInput>) {
        const command = new PutObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
        return this._client.send(command);
    }

    async put(key: string, data: string | Buffer | Uint8Array | Readable) {
        await this.putRaw(key, { Body: data });
    }

    async delRaw(key: string) {
        const command = new DeleteObjectCommand({
            Bucket: this.bucketName,
            Key: key,
        });
        return this._client.send(command);
    }

    async del(key: string) {
        await this.delRaw(key);
    }

    async copyRaw(oldKey: string, newKey: string, params?: Partial<CopyObjectCommandInput>) {
        const cmd = new CopyObjectCommand({
            Bucket: this.bucketName,
            CopySource: `${this.bucketName}/${oldKey}`,
            // rename
            Key: newKey,
            ...params,
        });
        return this._client.send(cmd);
    }

    async rename(oldKey: string, newKey: string) {
        if (oldKey === newKey) return;
        await this.copyRaw(oldKey, newKey);
        await this.delRaw(oldKey);
    }

    // -- Head

    async getHeadRaw(key: string, input?: Partial<HeadObjectCommandInput>): Promise<HeadObjectCommandOutput> {
        const command = new HeadObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
        return this._client.send(command);
    }

    async getHead(key: string): Promise<Partial<M>> {
        const s3response = await this.getHeadRaw(key);
        return (s3response.Metadata as Partial<M> | undefined) || {};
    }

    async getHeadsRaw(input?: Partial<ListObjectsCommandInput>): Promise<ListObjectsCommandOutput> {
        const command = new ListObjectsCommand({
            Bucket: this.bucketName,
            MaxKeys: 20,
            ...input,
        });
        return this._client.send(command);
    }

    async getHeads(options: FetchHeadesOptions) {
        const s3response = await this.getHeadsRaw();
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
        const currentMetadeata = await this.getHead(key);
        const newMetadata = this._config.mergeMetadata
            ? this._config.mergeMetadata(currentMetadeata, metadata)
            : metadata;
        await this.copyRaw(key, key, { MetadataDirective: "REPLACE", Metadata: parseMetadata(newMetadata) });
    }
}
