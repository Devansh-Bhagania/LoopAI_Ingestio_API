import { IngestionService } from '../services/ingestion';
import { Priority } from '../types';

async function seedDatabase() {
  const ingestionService = new IngestionService();

  // Generate a large array of IDs (e.g., 1000 IDs)
  const generateIds = (start: number, count: number) => {
    return Array.from({ length: count }, (_, i) => start + i);
  };

  // Create multiple ingestion requests with different priorities
  const highPriorityIds = generateIds(1, 20);
  const mediumPriorityIds = generateIds(21, 20);
  const lowPriorityIds = generateIds(41, 20);

  try {
    // Insert high priority data
    const highPriorityIngestionId = await ingestionService.createIngestion({
      ids: highPriorityIds,
      priority: Priority.HIGH
    });
    console.log('High priority ingestion created with ID:', highPriorityIngestionId);

    // Insert medium priority data
    const mediumPriorityIngestionId = await ingestionService.createIngestion({
      ids: mediumPriorityIds,
      priority: Priority.MEDIUM
    });
    console.log('Medium priority ingestion created with ID:', mediumPriorityIngestionId);

    // Insert low priority data
    const lowPriorityIngestionId = await ingestionService.createIngestion({
      ids: lowPriorityIds,
      priority: Priority.LOW
    });
    console.log('Low priority ingestion created with ID:', lowPriorityIngestionId);

    console.log('Database seeding completed successfully!');
  } catch (error) {
    console.error('Error seeding database:', error);
  }
}

seedDatabase(); 