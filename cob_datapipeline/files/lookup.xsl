<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema" exclude-result-prefixes="xs" version="2.0">

    <!-- called in TEU_XOAItoMARCXML.xsl to map language values; add additional lookups here as needed -->
    
    <xsl:template name="language">
        <xsl:param name="languageCode"/>
        <xsl:choose>
            <xsl:when test="$languageCode = 'es'">spa</xsl:when>
            <xsl:when test="$languageCode = 'eng'">eng</xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$languageCode"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
