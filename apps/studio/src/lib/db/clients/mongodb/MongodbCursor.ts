import { BeeCursor } from "../../models";
import rawLog from 'electron-log'
import { MongoClient } from 'mongodb';

const log = rawLog.scope('mongodbcursor');

export class MongodbCursor extends BeeCursor {

  private client?: MongoClient;

  constructor(
    conn: string,
    chunkSize: number
  ) {
    super(chunkSize);
    this.client = new MongoClient(conn);
  }

  async start(): Promise<void> {
    log.debug("start")
    this.client.connect();
  }

  async read(): Promise<any[][]> {
    const results = []

    return results
  }

  async cancel(): Promise<void> {
    log.debug('cursor cancelled');
    this.client.close();
  }
}
