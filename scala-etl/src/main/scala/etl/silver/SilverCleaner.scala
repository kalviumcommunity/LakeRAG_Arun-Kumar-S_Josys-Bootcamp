package etl.silver

object SilverCleaner {

  def cleanText(s: String): String = {
    if (s == null) return ""

    s.replaceAll("\\p{Cntrl}", " ")     // Remove control chars
      .replaceAll("<[^>]*>", " ")       // Remove HTML tags
      .replaceAll("&nbsp;", " ")        // Remove common HTML entity
      .replaceAll("\\s+", " ")          // Collapse whitespace
      .trim
      .toLowerCase                      // Normalize case
  }
}
