<?xml version="1.0" encoding="us-ascii"?>
<xsl:transform version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:v3="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" exclude-result-prefixes="v3 xsl">
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />
<xsl:strip-space elements="*" />

<xsl:template match="/">
    <PackageData> <!-- there can be multiple PRINCIPAL DISPLAY PANEL sections in a SPL -->
        <xsl:apply-templates select="//v3:section[v3:code[@code='51945-4']]"/>
    </PackageData>
</xsl:template>

<xsl:template match="//v3:section[v3:code[@code='51945-4']]">
        <MediaList> <!-- there can be multiple images within a PRINCIPAL DISPLAY PANEL section -->
            <xsl:for-each select=".//v3:observationMedia">
                <Media>
                    <ID>
                        <xsl:value-of select="./@ID"/>
                    </ID>
                    <Image>
                        <xsl:value-of select="v3:value/v3:reference/@value"/>
                    </Image>
                </Media>
            </xsl:for-each>
        </MediaList>
        <ID><xsl:value-of select=".//@root"/></ID>
        <Text><xsl:value-of select="."/></Text>
</xsl:template>

</xsl:transform>