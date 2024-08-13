<?xml version="1.0" encoding="us-ascii"?>
<xsl:transform version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:v3="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" exclude-result-prefixes="v3 xsl">
<xsl:output method="html" version="1.0" encoding="UTF-8" indent="yes" />
<xsl:strip-space elements="*" />

<xsl:template match="v3:document">
    <metadata>
        <documentId><xsl:value-of select="/v3:document/v3:id/@root"/></documentId>
        <SetId><xsl:value-of select="/v3:document/v3:setId/@root"/></SetId>
        <VersionNumber><xsl:value-of select="/v3:document/v3:versionNumber/@value"/></VersionNumber>
        <EffectiveDate><xsl:value-of select="/v3:document/v3:effectiveTime/@value"/></EffectiveDate>
        <MarketStatus><xsl:value-of select="//v3:subjectOf/v3:approval/v3:code/@displayName"/></MarketStatus>
        <ApplicationNumber><xsl:value-of select="//v3:subjectOf/v3:approval/v3:id/@extension"/></ApplicationNumber>
    </metadata>
</xsl:template>
    
</xsl:transform>