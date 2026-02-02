if (process.env.NODE_ENV !== 'development') { // Only silence in production (Claude Desktop)
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = (data: string | Uint8Array, ...args: any[]) => {
      if (!data.toString().includes('{"jsonrpc"')) { // Only filter non-JSON text
          console.error("STDOUT INTERFERENCE:", data.toString().trim()); 
      }
      return originalStdoutWrite(data, ...args);
  };
}

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  McpError,
  ErrorCode,
} from "@modelcontextprotocol/sdk/types.js";

import { MongoClient } from "mongodb";
import { Client } from "pg";
import { connectSqlite } from "./sqliteClient.js";


import dotenv from "dotenv";
if (!process.env.MONGODB_URI) {
  dotenv.config();
}

const uri = process.env.MONGODB_URI!;
const dbname = process.env.MONGODB_DB!;
const collectionName = process.env.MONGODB_COLLECTION!;

const mongoClient = new MongoClient(uri);
let mongoDb: any;
let collection: any;


// mongodb env's
// const uri = process.env.MONGODB_URI!;
// const dbname = process.env.MONGODB_DB!;
// const collectionName = process.env.MONGODB_COLLECTION!;
// const client = new MongoClient(uri);
// await client.connect();
// const db = client.db(dbname);
// const collection = db.collection(collectionName);

// postgres pg client

const pgClient = new Client({
  user: "postgres",
  host: "localhost",
  database: "mydb",
  password: "swago123@",
  port: 5432,
});

let pgConnected = false;


function ensureMongoReady() {
  if (!collection) {
    throw new McpError(
      ErrorCode.InternalError,
      "MongoDB not initialized yet"
    );
  }
}


async function initializeDatabases() {
  // MongoDB
  try {
    await mongoClient.connect();
    mongoDb = mongoClient.db(dbname);
    collection = mongoDb.collection(collectionName);
    console.error("✅ MongoDB connected");
  } catch (e) {
    console.error("❌ MongoDB failed", e);
  }

  // PostgreSQL
  try {
    if (!pgConnected) {
      await pgClient.connect();
      pgConnected = true;
      console.error("✅ PostgreSQL connected");
    }
  } catch (e) {
    console.error("❌ PostgreSQL failed", e);
  }

  // SQLite
  try {
    connectSqlite();
    console.error("✅ SQLite connected");
  } catch (e) {
    console.error("❌ SQLite failed", e);
  }
}


// pgClient
//   .connect()
//   .then(() => {
//     console.error("connected to thye postgres databse");
//   })
//   .catch((err: unknown) => {
//     console.error("posgtres databse not connected", err);
//   });

const server = new Server(
  {
    name: "mongo-mcp-server",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "create_document",
        description: "create a document in mongoDB collecion",
        inputSchema: {
          type: "object",
          properties: {
            doc: {
              type: "object",
              description: "Document to insert",
            },
          },
          required: ["doc"],
        },
      },
      {
        name: "read_document",
        description:
          "Read documents from MongoDB collection using a query filter",
        inputSchema: {
          type: "object",
          properties: {
            filter: {
              type: "object",
              description:
                "MongoDB query filter (leave empty to fetch all documents)",
            },
          },
          required: [], // filter is optional
        },
      },
      {
        name: "update_document",
        description: "Update documents in MongoDB collection using a filter",
        inputSchema: {
          type: "object",
          properties: {
            filter: {
              type: "object",
              description: "MongoDB query filter to select documents to update",
            },
            update: {
              type: "object",
              description:
                "MongoDB update object (e.g., { $set: { field: value } })",
            },
          },
          required: ["filter", "update"],
        },
      },
      {
        name: "delete_document",
        description: "Delete documents from MongoDB collection using a filter",
        inputSchema: {
          type: "object",
          properties: {
            filter: {
              type: "object",
              description: "MongoDB query filter to select documents to delete",
            },
          },
          required: ["filter"],
        },
      },
      {
        name: "create_table",
        description: "Create a new table in PostgreSQL with a given schema",
        inputSchema: {
          type: "object", // root must be an object
          properties: {
            tableName: {
              type: "string",
              description: "The name of the table to create",
            },
            columns: {
              type: "array", // columns is an array of strings
              description:
                "List of column definitions like 'id SERIAL PRIMARY KEY'",
              items: {
                type: "string",
              },
            },
          },
          required: ["tableName", "columns"], // both are required
          additionalProperties: false, // prevent extra fields
        },
      },
      {
        name: "insert_row",
        description: "insert  a row into a postgreSQL table",
        inputSchema: {
          type: "object",
          properties: {
            tableName: {
              type: "string",
              description: "name of the table the row will be inserted",
            },
            columns: {
              type: "array",
              description: "list of column name",
              items: { type: "string" },
            },
            values: {
              type: "array",
              description: "list of values matching the coloumn",
              items: {},
            },
          },
          required: ["tableName", "columns", "values"],
        },
      },
      {
        name: "read_rows",
        description: "Read rows from a PostgreSQL table with optional filters",
        inputSchema: {
          type: "object",
          properties: {
            tableName: {
              type: "string",
              description: "Name of the table to read from",
            },
            filter: {
              type: "object",
              description: "Optional key-value filter, e.g. { id: 1 }",
            },
          },
          required: ["tableName"],
        },
      },
      {
        name: "update_row",
        description: "Update rows in a PostgreSQL table using a filter",
        inputSchema: {
          type: "object",
          properties: {
            tableName: { type: "string", description: "Name of the table" },
            updates: {
              type: "object",
              description:
                "Columns and their new values, e.g. { name: 'John' }",
            },
            filter: {
              type: "object",
              description:
                "Filter for selecting rows to update, e.g. { id: 1 }",
            },
          },
          required: ["tableName", "updates", "filter"],
        },
      },
      {
        name: "delete_row",
        description: "Delete rows from a PostgreSQL table using a filter",
        inputSchema: {
          type: "object",
          properties: {
            tableName: { type: "string", description: "Name of the table" },
            filter: {
              type: "object",
              description: "Filter to select rows to delete, e.g. { id: 1 }",
            },
          },
          required: ["tableName", "filter"],
        },
      },
      {
        name: "create_table_sqlite",
        description: "create a new table in SQLite database with given schema",
        inputSchema: {
          type: "object",
          properties: {
            tableName: {
              type: "string",
              description: "the name of the table to create",
            },
            columns: {
              type: "array",
              description:
                "list of coloumn definition like 'id INTEGER PRIMARY KEY'",
              items: {
                type: "string",
              },
            },
          },
          required: ["tableName", "columns"],
          additionalProperties: false,
        },
      },
      {
        name: "insert_row_sqlite",
        description: "insert a row into a SQLite table",
        inputSchema: {
          type: "object",
          properties: {
            tableName: {
              type: "string",
              description: "The name of the SQLite table",
            },
            row: {
              type: "object",
              description: "key-value pairs representing the row data",
            },
          },
          required: ["tableName", "row"],
        },
      },
      {
        name: "update_row_sqlite",
        description: "Update rows in a SQLite table using a filter",
        inputSchema: {
          type: "object",
          properties: {
            tableName: {
              type: "string",
              description: "Name of the SQLite table",
            },
            updates: {
              type: "object",
              description:
                "Columns and their new values, e.g. { name: 'John' }",
            },
            filter: {
              type: "object",
              description: "Filter to select rows, e.g. { id: 1 }",
            },
          },
          required: ["tableName", "updates", "filter"],
        },
      },
      {
        name: "delete_row_sqlite",
        description: "Delete rows from a SQLite table using a filter",
        inputSchema: {
          type: "object",
          properties: {
            tableName: {
              type: "string",
              description: "Name of the SQLite table",
            },
            filter: {
              type: "object",
              description: "Filter to select rows, e.g. { id: 1 }",
            },
          },
          required: ["tableName", "filter"],
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  if (name === "create_document") {
    if (!args || typeof args !== "object" || !("doc" in args)) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "Missing or invalid arguments for create_document"
      );
    }

    const { doc } = args as { doc: Record<string, any> };

    try {
      const result = await collection.insertOne(doc as Record<string, any>);
      return {
        toolResult: { insertedId: result.insertedId.toString() },
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, "mongo db insert failed");
    }
  }

  if (name === "read_document") {
    if (!args || typeof args !== "object") {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "invalid arguments for read_document"
      );
    }

    const { filter } = args as { filter?: Record<string, any> };

    try {
      const docs = await collection.find(filter || {}).toArray();

      //  console.error("Read result:", docs);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(docs, null, 2),
          },
        ],
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, "mongodb read failed");
    }
  }

  if (name === "update_document") {
    if (
      !args ||
      typeof args != "object" ||
      !("filter" in args) ||
      !("update" in args)
    ) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "Missing or invalid arguments for update_document"
      );
    }

    const { filter, update } = args as {
      filter: Record<string, any>;
      update: Record<string, any>;
    };

    try {
      const result = await collection.updateMany(filter, update);

      return {
        toolResult: {
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        },
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, "mongodb update failed");
    }
  }

  if (name === "delete_document") {
    if (!args || typeof args !== "object" || !("filter" in args)) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "Missing or invalid arguments for delete_document"
      );
    }

    const { filter } = args as { filter: Record<string, any> };

    try {
      const result = await collection.deleteMany(filter);

      return {
        toolResult: {
          deletedCount: result.deletedCount,
        },
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, "mongodb delte failed");
    }
  }

  if (name === "create_table") {
    // cast args as any object, same as other tools
    const { tableName, columns } = args as {
      tableName: string;
      columns: string[];
    };

    if (!tableName || !tableName.trim()) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "Table name cannot be empty or whitespace"
      );
    }

    if (!Array.isArray(columns) || columns.length === 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "Columns must be a non-empty array of strings"
      );
    }

    const safeTableName = `"${tableName.trim().replace(/"/g, '""')}"`;

    try {
      const query = `CREATE TABLE ${safeTableName} (${columns.join(", ")});`;
      await pgClient.query(query);

      return {
        toolResult: {
          success: true,
          message: `Table '${tableName}' created successfully`,
        },
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `Postgres table creation failed: ${err.message}`
      );
    }
  }
  if (name === "insert_row") {
    if (
      !args ||
      typeof args !== "object" ||
      !("tableName" in args) ||
      !("columns" in args) ||
      !("values" in args)
    ) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing or inavalid arguments for insert_row"
      );
    }
    const { tableName, columns, values } = args as {
      tableName: string;
      columns: string[];
      values: any[];
    };

    try {
      const safeTableName = `"${tableName.trim().replace(/"/g, '""')}"`;
      const query = `INSERT INTO ${safeTableName} (${columns.join(
        ", "
      )}) VALUES (${columns
        .map((_, i) => `$${i + 1}`)
        .join(", ")}) RETURNING *;`;
      const result = await pgClient.query(query, values);

      return {
        toolResult: {
          success: true,
          insertedRow: result.rows[0],
        },
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `Postgres row insertion failed: ${err.message}`
      );
    }
  }
  if (name === "read_rows") {
    if (!args || typeof args !== "object" || !("tableName" in args)) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing or invalid arguments for read_rows"
      );
    }

    const { tableName, filter } = args as {
      tableName: string;
      filter?: Record<string, any>;
    };

    try {
      const safeTableName = `"${tableName.trim().replace(/"/g, '""')}"`;
      let query = `SELECT * FROM ${safeTableName}`;
      let values: any[] = [];

      if (
        filter &&
        typeof filter === "object" &&
        Object.keys(filter).length > 0
      ) {
        const conditions = Object.keys(filter)
          .map((key, i) => `"${key}" = $${i + 1}`)
          .join(" AND ");

        query += ` WHERE ${conditions}`;
        values = Object.values(filter);
      }

      const result = await pgClient.query(query, values);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result.rows, null, 2),
          },
        ],
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `Postgres read rows failed: ${err.message}`
      );
    }
  }
  if (name === "update_row") {
    if (
      !args ||
      typeof args !== "object" ||
      !("tableName" in args) ||
      !("updates" in args) ||
      !("filter" in args)
    ) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing or invalid arguments for update_row"
      );
    }

    const { tableName, updates, filter } = args as {
      tableName: string;
      updates: Record<string, any>;
      filter: Record<string, any>;
    };

    try {
      const safeTableName = `"${tableName.trim().replace(/"/g, '""')}"`;

      const updateKeys = Object.keys(updates);
      const updateValues = Object.values(updates);

      const filterKeys = Object.keys(filter);
      const filterValues = Object.values(filter);

      const setClause = updateKeys
        .map((key, i) => `"${key}" = $${i + 1}`)
        .join(", ");
      const whereClause = filterKeys
        .map((key, i) => `"${key}" = $${updateKeys.length + i + 1}`)
        .join(" AND ");

      const query = `UPDATE ${safeTableName} SET ${setClause} WHERE ${whereClause} RETURNING *;`;

      const result = await pgClient.query(query, [
        ...updateValues,
        ...filterValues,
      ]);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result.rows, null, 2),
          },
        ],
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `Postgres update row failed: ${err.message}`
      );
    }
  }
  if (name === "delete_row") {
    if (
      !args ||
      typeof args !== "object" ||
      !("tableName" in args) ||
      !("filter" in args)
    ) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing or invalid arguments for delete_row"
      );
    }

    const { tableName, filter } = args as {
      tableName: string;
      filter: Record<string, any>;
    };

    try {
      const safeTableName = `"${tableName.trim().replace(/"/g, '""')}"`;

      const filterKeys = Object.keys(filter);
      const filterValues = Object.values(filter);

      if (filterKeys.length === 0) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          "Filter cannot be empty for delete_row"
        );
      }

      const whereClause = filterKeys
        .map((key, i) => `"${key}" = $${i + 1}`)
        .join(" AND ");
      const query = `DELETE FROM ${safeTableName} WHERE ${whereClause} RETURNING *;`;

      const result = await pgClient.query(query, filterValues);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result.rows, null, 2),
          },
        ],
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `Postgres delete row failed: ${err.message}`
      );
    }
  }
  if (name === "create_table_sqlite") {
    const { tableName, columns } = args as {
      tableName: string;
      columns: string[];
    };

    if (!tableName || !columns?.length) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing tablename or columns for create_table_SQLite"
      );
    }

    try {
      const db = connectSqlite();
      const safeTableName = `"${tableName.replace(/"/g, '""')}"`;

      const sql = `CREATE TABLE ${safeTableName} (${columns.join(", ")});`;

      db.prepare(sql).run();

      return {
        toolResult: {
          success: true,
          message: `SQLite table ${tableName} created successfully `,
        },
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `sqlite table creation failed: ${err.message}`
      );
    }
  }
  if (name === "insert_row_sqlite") {
    const { tableName, row } = args as {
      tableName: string;
      row: Record<string, any>;
    };

    if (!tableName || !row || Object.keys(row).length === 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing tableName or row data for insert_row_sqlite"
      );
    }

    try {
      const db = connectSqlite();
      const columns = Object.keys(row);
      const placeholders = columns.map(() => "?").join(", ");
      const sql = `INSERT INTO "${tableName.replace(
        /"/g,
        '""'
      )}"(${columns.join(", ")}) VALUES (${placeholders});`;

      const stmt = db.prepare(sql);
      const result = stmt.run(...Object.values(row));

      return {
        toolResult: {
          success: true,
          message: `row inserted into ${tableName}`,
          changes: result.changes,
          lastInsertRowid: result.lastInsertRowid,
        },
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `sqlite row insertion failed: ${err.message}`
      );
    }
  }
  if (name === "update_row_sqlite") {
    const { tableName, updates, filter } = args as {
      tableName: string;
      updates: Record<string, any>;
      filter: Record<string, any>;
    };

    if (!tableName || !updates || !filter) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "missing tableName, updates or filter for update_row_sqlite"
      );
    }

    try {
      const db = connectSqlite();

      const updateKeys = Object.keys(updates);
      const updateValues = Object.values(updates);

      const filterKeys = Object.keys(filter);
      const filterValues = Object.values(filter);

      const setClause = updateKeys.map((key) => `"${key}" = ?`).join(", ");
      const whereClause = filterKeys.map((key) => `"${key}" = ?`).join(" AND ");

      const sql = `UPDATE "${tableName.replace(
        /"/g,
        '""'
      )}" SET ${setClause} WHERE ${whereClause};`;
      const stmt = db.prepare(sql);
      const result = stmt.run(...updateValues, ...filterValues);

      return {
        toolResult: {
          success: true,
          message: `rows updated in ${tableName}`,
          changes: result.changes,
        },
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `sqlite update row failed: ${err.message}`
      );
    }
  }
  if (name === "delete_row_sqlite") {
    const { tableName, filter } = args as {
      tableName: string;
      filter: Record<string, any>;
    };

    if (!tableName || !filter || Object.keys(filter).length === 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        "Missing tableName or filter for delete_row_sqlite"
      );
    }

    try {
      const db = connectSqlite();

      const filterKeys = Object.keys(filter);
      const filterValues = Object.values(filter);

      const whereClause = filterKeys.map((key) => `"${key}" = ?`).join(" AND ");
      const sql = `DELETE FROM "${tableName.replace(
        /"/g,
        '""'
      )}" WHERE ${whereClause};`;

      const stmt = db.prepare(sql);
      const result = stmt.run(...filterValues);

      return {
        toolResult: {
          success: true,
          message: `rows deleted from ${tableName}`,
          changes: result.changes,
        },
      };
    } catch (err: any) {
      throw new McpError(
        ErrorCode.InternalError,
        `sqlite delete row failed: ${err.message}`
      );
    }
  }

  throw new McpError(ErrorCode.InvalidRequest, "tool not found");
});

// const transport = new StdioServerTransport();
// await server.connect(transport);
const transport = new StdioServerTransport();
// await server.connect(transport);
// console.error("🚀 MCP server connected");

// initializeDatabases().catch(err => {
//   console.error("🔥 Database init failed", err);
//   process.exit(1);
// });
server.connect(transport)
  .then(() => {
    console.error("🚀 MCP server connected");
    initializeDatabases(); // Run this without 'await' in the main thread
  })
  .catch(err => {
    console.error("🔥 Server connection failed", err);
    process.exit(1); // Exit if connection fails
  });

