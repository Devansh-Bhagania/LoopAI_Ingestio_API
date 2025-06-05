import { syncDatabase } from '../models';

async function testDatabaseConnection() {
  try {
    console.log('Testing database connection...');
    await syncDatabase();
    console.log('Database connection and synchronization successful!');
  } catch (error) {
    console.error('Database connection or synchronization failed:', error);
  } finally {
    process.exit();
  }
}

testDatabaseConnection(); 