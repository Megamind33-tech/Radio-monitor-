import axios from 'axios';
import { logger } from '../lib/logger.js';

export class ResolverService {
  /**
   * Resolves a potentially indirect stream URL (M3U, PLS, M3U8) to a direct audio stream.
   */
  static async resolveStreamUrl(url: string): Promise<string> {
    logger.debug({ url }, "Resolving stream URL");
    
    try {
      const lowerUrl = url.toLowerCase();
      
      if (lowerUrl.endsWith('.m3u') || lowerUrl.endsWith('.m3u8')) {
        return await this.resolveM3U(url);
      } else if (lowerUrl.endsWith('.pls')) {
        return await this.resolvePLS(url);
      }
      
      // Default to direct if no known extension
      return url;
    } catch (error) {
      logger.warn({ url, error }, "Failed to resolve stream URL, using original");
      return url;
    }
  }

  private static async resolveM3U(url: string): Promise<string> {
    const response = await axios.get(url, { timeout: 5000 });
    const content = response.data as string;
    const lines = content.split('\n');
    
    // Find the first line that is a URL and not a comment
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith('#')) {
        logger.info({ original: url, resolved: trimmed }, "Resolved M3U stream");
        return trimmed;
      }
    }
    
    return url;
  }

  private static async resolvePLS(url: string): Promise<string> {
    const response = await axios.get(url, { timeout: 5000 });
    const content = response.data as string;
    
    // Basic PLS parsing: File1=http://...
    const match = content.match(/^File1=(.*)$/m);
    if (match && match[1]) {
      const resolved = match[1].trim();
      logger.info({ original: url, resolved }, "Resolved PLS stream");
      return resolved;
    }
    
    return url;
  }
}
