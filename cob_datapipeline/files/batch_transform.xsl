<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dcterms="http://purl.org/dc/terms/"
                xmlns:dpla="http://dp.la/about/map/"
                xmlns:edm="http://www.europeana.eu/schemas/edm/"
                xmlns:oai="http://www.openarchives.org/OAI/2.0/"
                xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
                xmlns:oclc="http://purl.org/oclc/terms/"
                xmlns:oclcdc="http://worldcat.org/xmlschemas/oclcdc-1.0/"
                xmlns:oclcterms="http://purl.org/oclc/terms/"
                xmlns:schema="http://schema.org"
                xmlns:padig="http://padigital.org/ns"
                xmlns:svcs="http://rdfs.org/sioc/services"
                exclude-result-prefixes="xs"
                version="2.0">
  <!-- Copy anything as it is -->
  <xsl:template match="node() | @*">
    <xsl:copy>
      <xsl:apply-templates select="node() | @*"/>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="/collection/*">
    <xsl:copy>
      <xsl:apply-templates select="@*"/>
      <xsl:attribute name="airflow-record-id" select="dcterms:identifier"/>
      <xsl:apply-templates select="node()"/>
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>
