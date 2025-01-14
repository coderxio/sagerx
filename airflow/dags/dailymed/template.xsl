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

        <PackageLabels>
            <xsl:apply-templates select="//v3:section[v3:code[@code='51945-4']]"/>
        </PackageLabels>

        <NDCList>
            <xsl:apply-templates select="v3:document/v3:component"/>
        </NDCList>
        
        <InteractionText>
            <xsl:apply-templates select="//v3:code[@code='34073-7']"/>
        </InteractionText>
        
        <Organizations>
            <xsl:apply-templates select="/v3:document/v3:author/v3:assignedEntity/v3:representedOrganization"/>
            <xsl:choose>
                <xsl:when test="//v3:text[contains(.,'Manufactured by')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Manufactured by')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'Manufactured By')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Manufactured By')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'manufactured by')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'manufactured by')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'Manufactured For')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Manufactured For')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'Manufactured for')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Manufactured for')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'MANUFACTURED For')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'MANUFACTURED For')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'Distributor')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Distributor')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'Manufactured in')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Manufactured in')]"/>
                </xsl:when>
                <xsl:when test="//v3:text[contains(.,'Made in')]">
                    <xsl:apply-templates select="//v3:text[contains(.,'Made in')]"/>
                </xsl:when>
            </xsl:choose>
            
        </Organizations>
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

<!-- PackageLabels -->
<xsl:template match="//v3:section[v3:code[@code='51945-4']]">
    <PackageLabel> <!-- there can be multiple PRINCIPAL DISPLAY PANEL sections in a SPL -->
        <MediaList>
            <!-- If v3:observationMedia exists, process only those -->
            <xsl:choose>
                <xsl:when test=".//v3:observationMedia">
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
                </xsl:when>
                <!-- Else, process v3:renderMultiMedia -->
                <xsl:otherwise>
                    <xsl:for-each select=".//v3:renderMultiMedia">
                        <xsl:variable name="refID" select="@referencedObject"/>
                        
                        <!-- look for the corresponding observationMedia with the same ID -->
                        <xsl:for-each select="//v3:observationMedia[@ID=$refID]">
                            <Media>
                                <ID>
                                    <xsl:value-of select="./@ID"/>
                                </ID>
                                <Image>
                                    <xsl:value-of select="v3:value/v3:reference/@value"/>
                                </Image>
                            </Media>
                        </xsl:for-each>
                    </xsl:for-each>
                </xsl:otherwise>
            </xsl:choose>
        </MediaList>
        <ID><xsl:value-of select=".//@root"/></ID>
        <Text><xsl:value-of select="."/></Text>
    </PackageLabel>
</xsl:template>

<!-- NDCList -->
<xsl:template match="v3:document/v3:component">
    <xsl:for-each select="//v3:containerPackagedProduct/v3:code[@codeSystem='2.16.840.1.113883.6.69']">  
        <NDC><xsl:value-of select="./@code"/></NDC>
    </xsl:for-each>
</xsl:template>

<!--InteractionText -->
<xsl:template match="//v3:code[@code='34073-7']"> 
        <xsl:value-of select=".."/>
</xsl:template>

<!-- Organizations (establishment) -->
<xsl:template match="/v3:document/v3:author/v3:assignedEntity/v3:representedOrganization">
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
</xsl:template>
<!-- 57237-007-05 for registrant -->

<!-- Organization Text --> 
<xsl:template match="//v3:text[contains(.,'Manufactured For')]|//v3:text[contains(.,'Manufactured for')]|//v3:text[contains(.,'Manufactured by')]|//v3:text[contains(.,'manufactured by')]|//v3:text[contains(.,'Made in')]|//v3:text[contains(.,'Manufactured By')]|//v3:text[contains(.,'MANUFACTURED For')]|//v3:text[contains(.,'Distributor')]|//v3:text[contains(.,'Manufactured in')]|//v3:text[contains(.,'Made in')]">
    <OrganizationsText>
        <xsl:value-of select="."/>
    </OrganizationsText>
</xsl:template>
<!-- Distributed By  Repackaged By    -->
</xsl:transform>
