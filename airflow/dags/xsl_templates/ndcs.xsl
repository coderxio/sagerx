<?xml version="1.0" encoding="us-ascii"?>
<xsl:transform version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:v3="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" exclude-result-prefixes="v3 xsl">
<xsl:output method="html" version="1.0" encoding="UTF-8" indent="yes" />
<xsl:strip-space elements="*" />

    <xsl:template match="/">
        <results>
            <xsl:apply-templates select="//*[contains(@codeSystem, '2.16.840.1.113883.6.69')]"/>
        </results>
    </xsl:template>

    <xsl:template match="*[@codeSystem = '2.16.840.1.113883.6.69' or @codeSystem = '2.16.840.1.113883.3.6277']">
        <NDC>
            <xsl:value-of select="@code"/>
        </NDC>
    </xsl:template>

</xsl:transform>