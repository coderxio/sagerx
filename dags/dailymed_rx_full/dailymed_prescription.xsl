<xsl:transform version="1.0" 
            				 xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
							 xmlns:v3="urn:hl7-org:v3" 
							 xmlns:v="http://validator.pragmaticdata.com/result" 
							 xmlns:str="http://exslt.org/strings" 
							 xmlns:exsl="http://exslt.org/common" 
							 xmlns:msxsl="urn:schemas-microsoft-com:xslt" 
							 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
							 exclude-result-prefixes="exsl msxsl v3 xsl xsi str v">                
<xsl:output method="xml" indent="yes"/> 

<xsl:template match="/">
    <dailymed>
        <xsl:apply-templates select="v3:document"/>
        <xsl:apply-templates select="v3:document/v3:component"/>
        <xsl:apply-templates select="//v3:code[@code='34073-7']"/>
        <xsl:apply-templates select="/v3:document/v3:author/v3:assignedEntity/v3:representedOrganization"/>
    </dailymed>
</xsl:template>

<!-- header --> 
<xsl:template match="v3:document">
    <documentId><xsl:value-of select="/v3:document/v3:id/@root"/></documentId>
    <SetId><xsl:value-of select="/v3:document/v3:setId/@root"/></SetId>
    <VersionNumber><xsl:value-of select="/v3:document/v3:versionNumber/@value"/></VersionNumber>
    <EffectiveDate><xsl:value-of select="/v3:document/v3:effectiveTime/@value"/></EffectiveDate>
    <MarketStatus><xsl:value-of select="//v3:subjectOf/v3:approval/v3:code/@displayName"/></MarketStatus>
    <ApplicationNumber><xsl:value-of select="//v3:subjectOf/v3:approval/v3:id/@extension"/></ApplicationNumber>
</xsl:template>

<!-- NDC -->
<xsl:template match="v3:document/v3:component">
    <ndc_list>
    <xsl:for-each select="//v3:containerPackagedProduct/v3:code[@codeSystem='2.16.840.1.113883.6.69']">  
        <NDC><xsl:value-of select="./@code"/></NDC>
    </xsl:for-each>
    </ndc_list>
</xsl:template>

<!--Drug Interactions -->
<xsl:template match="//v3:code[@code='34073-7']"> 
    <InteractionText>
        <xsl:value-of select=".."/>
    </InteractionText>

</xsl:template>

<!-- establishment info -->
<xsl:template match="/v3:document/v3:author/v3:assignedEntity/v3:representedOrganization">
    <Organizations> 
        <establishment>
            <DUN><xsl:value-of select="./v3:id/@extension"/></DUN>
            <name><xsl:value-of select="./v3:name"/></name>
            <xsl:choose> 
                <xsl:when test="//v3:asEquivalentEntity/v3:code/@code = 'C64637'">
                    <type>Repacker</type>
                    <source_list>
                    <xsl:for-each select="//v3:asEquivalentEntity/v3:code[@code='C64637']/../v3:definingMaterialKind/v3:code">
                        <source><xsl:value-of select="./@code"/></source>
                    </xsl:for-each>
                    </source_list>
                </xsl:when>
                <xsl:otherwise>
                    <type>Labeler</type>  
                </xsl:otherwise>
            </xsl:choose>
        </establishment>
        
        <xsl:for-each select="./v3:assignedEntity/v3:assignedOrganization/v3:assignedEntity/v3:assignedOrganization">
        <establishment>
            <DUN><xsl:value-of select="./v3:id[@root='1.3.6.1.4.1.519.1']/@extension"/><xsl:if test="./v3:id[@root='1.3.6.1.4.1.519.1']/@extension and ./v3:id[not(@root='1.3.6.1.4.1.519.1')]/@extension">/</xsl:if><xsl:value-of select="./v3:id[not(@root='1.3.6.1.4.1.519.1')]/@extension"/></DUN>
            <name><xsl:value-of select="./v3:name"/></name>
            <type>Functioner</type>
                <xsl:for-each select="../v3:performance[not(v3:actDefinition/v3:code/@code = preceding-sibling::*/v3:actDefinition/v3:code/@code)]/v3:actDefinition/v3:code">
                    <xsl:variable name="code" select="@code"/>
                    <function>
                        <name><xsl:value-of select="@displayName"/></name>
                        <item_list>
                        	    <xsl:for-each select="../../../v3:performance/v3:actDefinition[v3:code/@code = $code]/v3:product/v3:manufacturedProduct/v3:manufacturedMaterialKind/v3:code/@code">
								<item><xsl:value-of select="."/></item>
                              </xsl:for-each>
                        </item_list>
                    </function>
                </xsl:for-each>
        </establishment>  
        </xsl:for-each>
       </Organizations> 
</xsl:template>

</xsl:transform>
