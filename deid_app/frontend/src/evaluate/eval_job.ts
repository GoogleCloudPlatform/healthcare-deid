/**
 * Represents an Eval job along with the information needed for that job.
 */
export interface EvalJob {
  id: number;
  name: string;
  findings: string;
  goldens: string;
  stats: string;
  debug: string;
  status: number;
  logTrace: string;
  timestamp: Date;
}

/**
 * Represents an Eval stats entry.
 */
export interface EvalStats {
  infoType: string;
  recall?: number;
  precision?: number;
  fScore?: number;
  truePositives?: number;
  falsePositives?: number;
  falseNegatives?: number;
  timestamp: Date;
}
