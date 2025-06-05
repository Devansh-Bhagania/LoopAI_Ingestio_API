import { Sequelize, Model, DataTypes } from 'sequelize';
import { BatchStatus } from '../types';

const sequelize = new Sequelize({
  dialect: 'postgres',
  host: 'db.aksbefowkaxukxvqrxdj.supabase.co',
  port: 5432,
  username: 'postgres',
  password: 'yHx95_9FkYEY?xW',
  database: 'postgres',
  dialectOptions: {
    ssl: {
      require: true,
      rejectUnauthorized: false
    }
  },
  logging: false,
  pool: {
    max: 5,
    min: 0,
    acquire: 30000,
    idle: 10000
  }
});

export class Ingestion extends Model {
  public ingestion_id!: string;
  public status!: BatchStatus;
  public readonly created_at!: Date;
}

export class Batch extends Model {
  public batch_id!: string;
  public ingestion_id!: string;
  public ids!: number[];
  public status!: BatchStatus;
  public readonly created_at!: Date;
}

Ingestion.init(
  {
    ingestion_id: {
      type: DataTypes.STRING,
      primaryKey: true
    },
    status: {
      type: DataTypes.STRING,
      allowNull: false
    },
    created_at: {
      type: DataTypes.DATE,
      defaultValue: DataTypes.NOW
    }
  },
  {
    sequelize,
    tableName: 'ingestions',
    timestamps: false
  }
);

Batch.init(
  {
    batch_id: {
      type: DataTypes.STRING,
      primaryKey: true
    },
    ingestion_id: {
      type: DataTypes.STRING,
      allowNull: false,
      references: {
        model: Ingestion,
        key: 'ingestion_id'
      }
    },
    ids: {
      type: DataTypes.ARRAY(DataTypes.INTEGER),
      allowNull: false
    },
    status: {
      type: DataTypes.STRING,
      allowNull: false
    },
    created_at: {
      type: DataTypes.DATE,
      defaultValue: DataTypes.NOW
    }
  },
  {
    sequelize,
    tableName: 'batches',
    timestamps: false
  }
);

// Define relationships
Ingestion.hasMany(Batch, { foreignKey: 'ingestion_id' });
Batch.belongsTo(Ingestion, { foreignKey: 'ingestion_id' });

export const syncDatabase = async () => {
  try {
    await sequelize.authenticate();
    console.log('Database connection has been established successfully.');
    
    // Sync all models
    await sequelize.sync({ alter: true });
    console.log('Database synchronized successfully.');
  } catch (error) {
    console.error('Unable to connect to the database:', error);
    throw error;
  }
};

export default sequelize; 