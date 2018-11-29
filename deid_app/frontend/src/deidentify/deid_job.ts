/**
 * Represents a Deid job along with the information needed for that job.
 */
export interface DeidJob {
  id: number;
  name: string;
  originalQuery: string;
  deidTable: string;
  status?: number;
  logTrace?: string;
  timestamp: Date;
}
