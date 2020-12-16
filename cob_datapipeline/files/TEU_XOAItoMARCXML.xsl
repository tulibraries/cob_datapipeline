<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns:dcterms="http://purl.org/dc/terms/1.1"
    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.lyncode.com/xoai http://www.lyncode.com/xsd/xoai.xsd"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://www.loc.gov/MARC21/slim"  exclude-result-prefixes="dc dcterms oai_dc">

    <xsl:import href="loc_file.xsl"/>
    <xsl:output method="xml" encoding="UTF-8" indent="yes"/>

    <xsl:template match="/">
        <collection>
            <xsl:apply-templates />
        </collection>
    </xsl:template>

    <!--
    <xsl:template name="OAI-PMH">
        <xsl:for-each select = "ListRecords/record/metadata/xoai">
            <xsl:apply-templates  />
        </xsl:for-each>
        <xsl:for-each select = "GetRecord/record/metadata/xoai">
            <xsl:apply-templates  />
        </xsl:for-each>
    </xsl:template>-->

    <xsl:template match="text()" />
    <xsl:template match="element[@name='dc']">
        <record>
            <xsl:element name="leader">
                <xsl:variable name="leader06">a</xsl:variable>
                <xsl:variable name="leader07">m</xsl:variable>
                <xsl:value-of select="concat('      ',$leader06,$leader07,'         Ki     ')"/>
            </xsl:element>

            <!-- Variables -->

            <xsl:variable name="thesisDate" select="//element[@name='date']/element[@name='issued'][1]/element/field[@name='value']" />
            <xsl:variable name="thesisYear">
                <xsl:value-of select="substring($thesisDate,1,4)"/>
            </xsl:variable>
            <xsl:variable name="thesisLanguage" select="element[@name='language']/element[@name='iso']/element/field[@name='value']" />
            <xsl:variable name="thesisTitle" select="element[@name='title']/element/field[@name='value'][1]" />
            <xsl:variable name="thesisAuthor" select="element[@name='creator']/element/field[@name='value']" />
            <xsl:variable name="authorORCID" select="element[@name='creator']/element[@name='orcid']/element/field[@name='value']"/>
            <xsl:variable name="embargo" select="element[@name='embargo']/element[@name='lift']/element/field[@name='value']" />

            <!-- Control Fields -->

            <controlfield tag="006">
                <xsl:text xml:space="preserve">m\\\\\o\\d\\\\\\\\</xsl:text>
            </controlfield>

            <controlfield tag="007">
                <xsl:text>cr</xsl:text>
            </controlfield>

            <controlfield tag="008">
                <xsl:text>000000</xsl:text>
                <xsl:text xml:space="preserve">s</xsl:text>
                <xsl:value-of select="$thesisYear"/>
                <xsl:text xml:space="preserve">\\\\pau\\\\\obm\\\000\0\</xsl:text>
                <xsl:value-of select="$thesisLanguage"/>
                <xsl:text xml:space="preserve">\d</xsl:text>
            </controlfield>

            <!-- Variable Length Fields -->

            <!-- Temple Identifier -->
            
            <xsl:if test="element[@name='identifier']/element[@name='filename']">

            <xsl:variable name="filename" select="element[@name='identifier']/element[@name='filename']/element/field[@name='value']"/>
            <xsl:variable name="filename-id" select="substring-before($filename,'.pdf')"/>

            <datafield tag="024" ind1="8" ind2=" ">
                <subfield code="a">
                    <xsl:value-of select="$filename-id" />
                </subfield>
            </datafield>
                
            </xsl:if>

            <!-- ProQuest ETD Admin Identifier -->
            
            <xsl:if test="element[@name='identifier']/element[@name='proqst']">

            <datafield tag="035" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:text xml:space="preserve">(MiAaPQD)</xsl:text>
                    <xsl:value-of select="element[@name='identifier']/element[@name='proqst']/element/field[@name='value']"/>
                </subfield>
            </datafield>
                
            </xsl:if>
            
            <!-- DOI-based identifier -->
            
            <xsl:variable name="doi1" select="element[@name='relation']/element[@name='doi']/element/field[@name='value']"/>
            <xsl:variable name="itemID" select="replace(substring-after($doi1,'doi.org/'),'[^a-zA-Z0-9\-:_]','_')"/>
            
            <datafield tag="035" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:text xml:space="preserve">(PPT)</xsl:text>
                    <xsl:value-of select="$itemID"/>
                </subfield>
            </datafield>

            <!-- Author -->

            <xsl:for-each select="element[@name='creator']/element/field[@name='value']">
                <xsl:choose>
                    <xsl:when test="(.!='') and (position()=1)">
                        <xsl:call-template name="persname_template">
                            <xsl:with-param name="string" select="." />
                            <xsl:with-param name="field" select="'100'" />
                            <xsl:with-param name="ind1" select = "'1'" />
                            <xsl:with-param name="ind2" select = "' '" />
                            <xsl:with-param name="type" select="'author'" />
                            <xsl:with-param name="orcid" select="$authorORCID"/>
                        </xsl:call-template>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:if test=".!=''">
                            <xsl:call-template name="persname_template">
                                <xsl:with-param name="string" select="." />
                                <xsl:with-param name="field" select="'700'" />
                                <xsl:with-param name="ind1" select = "'1'" />
                                <xsl:with-param name="ind2" select = "' '" />
                                <xsl:with-param name="type" select="'author'" />
                                <xsl:with-param name="orcid" select="$authorORCID"/>
                            </xsl:call-template>
                        </xsl:if>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>

            <!-- Title -->

            <!-- Notes:
            Due to a DSpace error, currently this is title[1]
            The [1] will not be necessary once that is fixed.
            While sentence case conversion is possible, it may note be desirable due to
            lack of detection for proper names, which are common.-->

            <datafield tag="245" ind1="1">
                <xsl:attribute name="ind2">
                    <xsl:choose>
                        <xsl:when test="starts-with($thesisTitle, 'The ')">4</xsl:when>
                        <xsl:when test="starts-with($thesisTitle, 'An ')">3</xsl:when>
                        <xsl:when test="starts-with($thesisTitle, 'A ')">2</xsl:when>
                        <xsl:otherwise>0</xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
                <xsl:if test="contains($thesisTitle, ':')">
                    <subfield code="a">
                        <xsl:value-of
                            select="substring-before($thesisTitle, ':')"
                        />
                        <xsl:text xml:space="preserve"> : </xsl:text>
                    </subfield>
                    <subfield code="b">
                        <xsl:value-of
                            select="substring-after($thesisTitle, ':')"
                        />
                        <xsl:text xml:space="preserve"> /</xsl:text>
                    </subfield>
                    <subfield code="c">
                        <xsl:if test="contains($thesisAuthor, ',')">
                            <xsl:text xml:space="preserve">by </xsl:text>
                            <xsl:value-of select="substring-after($thesisAuthor,',')"/>
                            <xsl:text xml:space="preserve"> </xsl:text>
                            <xsl:value-of select="substring-before($thesisAuthor,',')"/>
                            <xsl:text>.</xsl:text>
                        </xsl:if>
                        <xsl:if test="not (contains($thesisAuthor, ','))">
                            <xsl:text xml:space="preserve">by</xsl:text>
                            <xsl:value-of select="$thesisAuthor"/>
                            <xsl:text>.</xsl:text>
                        </xsl:if>
                    </subfield>
                </xsl:if>
                <xsl:if test="not (contains($thesisTitle, ':'))">
                    <subfield code="a">
                        <xsl:value-of select="$thesisTitle"/>
                        <xsl:text xml:space="preserve"> /</xsl:text>
                    </subfield>
                    <subfield code="c">
                        <xsl:if test="contains($thesisAuthor, ',')">
                            <xsl:text xml:space="preserve">by</xsl:text>
                            <xsl:value-of select="substring-after($thesisAuthor,',')"/>
                            <xsl:text xml:space="preserve"> </xsl:text>
                            <xsl:value-of select="substring-before($thesisAuthor,',')"/>
                            <xsl:text>.</xsl:text>
                        </xsl:if>
                        <xsl:if test="not (contains($thesisAuthor, ','))">
                            <xsl:text xml:space="preserve">by</xsl:text>
                            <xsl:value-of select="$thesisAuthor"/>
                            <xsl:text>.</xsl:text>
                        </xsl:if>
                    </subfield>
                </xsl:if>
            </datafield>

            <!-- Publication Statement -->

            <datafield tag="264" ind1=" " ind2="1">
                <subfield code="a">
                    <xsl:text xml:space="preserve">[Philadelphia, Pennsylvania] :</xsl:text>
                </subfield>
                <subfield code="b">
                    <xsl:text xml:space="preserve">Temple University Libraries, </xsl:text>
                </subfield>
                <subfield code="c">
                    <xsl:value-of select="$thesisYear" />.
                </subfield>
            </datafield>

            <!-- Physical Description -->

            <xsl:variable name="extent" select="element[@name='format']/element[@name='extent']/element/field[@name='value']"/>

            <datafield tag="300" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:text xml:space="preserve" >1 online resource (</xsl:text>
                    <xsl:value-of select="$extent"/>
                    <xsl:text xml:space="preserve">).</xsl:text>
                </subfield>
            </datafield>

            <datafield tag="336" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:text>text</xsl:text>
                </subfield>
                <subfield code="2">
                    <xsl:text>rdacontent</xsl:text>
                </subfield>
            </datafield>

            <datafield tag="337" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:text>computer</xsl:text>
                </subfield>
                <subfield code="2">
                    <xsl:text>rdamedia</xsl:text>
                </subfield>
            </datafield>

            <datafield tag="338" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:text>online resource</xsl:text>
                </subfield>
                <subfield code="2">
                    <xsl:text>rdacarrier</xsl:text>
                </subfield>
            </datafield>

            <!-- Bibliography Note -->

            <datafield tag="504" ind1=" " ind2=" ">
                <subfield code="a"><xsl:text xml:space="preserve" >Includes bibliographical references.</xsl:text></subfield>
            </datafield>

            <!-- Thesis Note -->

            <datafield tag="502" ind1=" " ind2=" ">
                <subfield code="b">
                    <xsl:value-of select="element[@name='description']/element[@name='degree']/element/field[@name='value']"/>
                </subfield>
                <subfield code="c">Temple University</subfield>
                <subfield code="d">
                    <xsl:value-of select="$thesisYear"/>
                </subfield>
            </datafield>
            
            <xsl:if test="element[@name='description']/element[@name='schoolcollege']">
                <datafield tag="500" ind1=" " ind2=" ">
                    <subfield code="a">
                        <xsl:value-of select="element[@name='description']/element[@name='schoolcollege']/element/field[@name='value']"/>
                    </subfield>
                </datafield>
            </xsl:if>

            <!-- Abstract -->

            <!-- Note:
            Due a DSpace error this is set to only map abtract[1]
            Once this error is fixed the [1] can be removed-->

            <xsl:for-each select="element[@name='description']/element[@name='abstract']/element/field[@name='value'][1]">
                <datafield tag="520" ind1=" " ind2=" ">
                    <subfield code="a">
                        <xsl:value-of select="normalize-space(.)"/>
                    </subfield>
                </datafield>
            </xsl:for-each>

            <!-- Rights -->

            <datafield tag="540" ind1=" " ind2=" ">
                <subfield code="a">
                    <xsl:value-of select="element[@name='rights']/element/field[@name='value']"/>
                </subfield>
                <subfield code="u">
                    <xsl:value-of select="element[@name='rights']/element[@name='uri']/element/field[@name='value']"/>
                </subfield>
            </datafield>

            <!-- Subjects -->

            <!-- Unfortunately our DSpace instance does not differentiate between ProQuest controlled subject terms and uncontrolled keywords.
            If we map them, either all could go to 650#4, all to 653, or maybe 1st to 650#4 and the rest to 653?
            We have no way of knowing how many controlled terms there are so other than 1st we really don't know which is which.
            Or, we could map none of them.
            For now, they are all mapped to 653.-->


                <xsl:for-each select="element[@name='subject']/element/field[@name='value']">
                    <xsl:call-template name="keyword_template">
                        <xsl:with-param name="field" select="'653'" />
                        <xsl:with-param name="ind1" select="' '" />
                        <xsl:with-param name="ind2" select="' '" />
                        <xsl:with-param name="string" select="." />
                        <xsl:with-param name="delimiter" select="','" />
                    </xsl:call-template>
                </xsl:for-each>


            <!-- Local practice has been to map the department name to a local subject heading with
            a subfield x for Temple University theses-->
            
            <xsl:if test="element[@name='description']/element[@name='schoolcollege']">

            <datafield tag="690" ind1=" " ind2="4">
                <subfield code="a">
                    <xsl:value-of select="element[@name='description']/element[@name='schoolcollege']/element/field[@name='value']"/>
                </subfield>
                <subfield code="x">
                    <xsl:text xml:space="preserve">Temple University theses.</xsl:text>
                </subfield>
            </datafield>
                
            </xsl:if>

            <!-- Standardized local subject heading for Temple theses -->

            <datafield tag="690" ind1=" " ind2="4">
                <subfield code="a">
                    <xsl:text xml:space="preserve">Temple University</xsl:text>
                </subfield>
                <subfield code="x">
                    <xsl:text xml:space="preserve">Theses.</xsl:text>
                </subfield>
            </datafield>

            <!-- Genre -->

            <datafield tag="655" ind1=" " ind2="7">
                <subfield code="a">Academic theses.</subfield>
                <subfield code="2">lcgft</subfield>
            </datafield>

            <!-- Advisors and Committee Members -->

            <xsl:for-each select="element[@name='contributor']/element[@name='advisor']/element/field[@name='value']">
                <xsl:call-template name="persname_template">
                    <xsl:with-param name="string" select="." />
                    <xsl:with-param name="field" select="'700'" />
                    <xsl:with-param name="ind1" select = "'1'" />
                    <xsl:with-param name="ind2" select = "' '" />
                    <xsl:with-param name="type" select="'advisor'" />
                </xsl:call-template>
            </xsl:for-each>

            <xsl:for-each select="element[@name='contributor']/element[@name='committeemember']/element/field[@name='value']">
                <xsl:call-template name="persname_template">
                    <xsl:with-param name="string" select="." />
                    <xsl:with-param name="field" select="'700'" />
                    <xsl:with-param name="ind1" select = "'1'" />
                    <xsl:with-param name="ind2" select = "' '" />
                    <xsl:with-param name="type" select="'committee member'" />
                </xsl:call-template>
            </xsl:for-each>

            <!-- URL -->

            <xsl:variable name="hdl" select="element[@name='identifier']/element[@name='uri']/element/field[@name='value']"/>
            <xsl:variable name="doi" select="element[@name='relation']/element[@name='doi']/element/field[@name='value']"/>

            <datafield tag="856" ind1="4" ind2="0">
                <subfield code="u">
                    <xsl:choose>
                        <xsl:when test="$doi">
                            <xsl:value-of select="$doi"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$hdl"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </subfield>
                <subfield code="z">Access online resource.</subfield>
            </datafield>

            <!-- Local note to flag embargoes -->

            <xsl:if test="$embargo">
                <datafield tag="902" ind1=" " ind2=" ">
                    <subfield code="a">
                        <xsl:text xml:space="preserve">EMBARGO LIFT: </xsl:text>
                        <xsl:value-of select="$embargo"/>
                    </subfield>
                </datafield>
            </xsl:if>

            <!-- End Variable Length Fields -->

        </record>
    </xsl:template>

<!--Keyword template-->

    <xsl:template name="keyword_template">
        <xsl:param name="field" />
        <xsl:param name="ind1" />
        <xsl:param name="ind2" />
        <xsl:param name="string" />
        <xsl:param name="delimiter" />

        <xsl:choose>
            <!-- IF A PAREN, STOP AT AN OPENING semicolon -->
            <xsl:when test="contains($string, $delimiter)">
                <xsl:variable name="newstem" select="substring-after($string, $delimiter)" />
                <datafield>
                    <xsl:attribute name="tag">
                        <xsl:value-of select="$field" />
                    </xsl:attribute>

                    <xsl:attribute name="ind1">
                        <xsl:value-of select="$ind1" />
                    </xsl:attribute>

                    <xsl:attribute name="ind2">
                        <xsl:value-of select="$ind2" />
                    </xsl:attribute>
                    <subfield code="a">
                        <xsl:value-of select="substring-before($string, $delimiter)" />
                    </subfield>
                </datafield>
                <!--Need to do recursion-->
                <xsl:call-template name="keyword_template">
                    <xsl:with-param name="field" select="'653'" />
                    <xsl:with-param name="ind1" select="' '" />
                    <xsl:with-param name="ind2" select="' '" />
                    <xsl:with-param name="string" select="normalize-space($newstem)" />
                    <xsl:with-param name="delimiter" select="','" />
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <datafield>
                    <xsl:attribute name="tag">
                        <xsl:value-of select="$field" />
                    </xsl:attribute>

                    <xsl:attribute name="ind1">
                        <xsl:value-of select="$ind1" />
                    </xsl:attribute>

                    <xsl:attribute name="ind2">
                        <xsl:value-of select="$ind2" />
                    </xsl:attribute>
                    <subfield code="a">
                        <xsl:value-of select="$string" />
                    </subfield>
                </datafield>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

<!-- Personal Name Template -->

    <xsl:template name="persname_template">
        <xsl:param name="string" />
        <xsl:param name="field" />
        <xsl:param name="ind1" />
        <xsl:param name="ind2" />
        <xsl:param name="type" />
        <xsl:param name="orcid" />
        <datafield>
            <xsl:attribute name="tag">
                <xsl:value-of select="$field" />
            </xsl:attribute>
            <xsl:attribute name="ind1">
                <xsl:value-of select="$ind1" />
            </xsl:attribute>
            <xsl:attribute name="ind2">
                <xsl:value-of select="$ind2" />
            </xsl:attribute>

            <!-- Sample input: Brightman, Samuel C. (Samuel Charles), 1911-1992 -->
            <!-- Sample output: $aBrightman, Samuel C. $q(Samuel Charles), $d1911-. -->
            <!-- will handle names with dashes e.g. Bourke-White, Margaret -->

            <!-- CAPTURE PRIMARY NAME BY LOOKING FOR A PAREN OR A DASH OR NEITHER -->
            <xsl:choose>
                <!-- IF A PAREN, STOP AT AN OPENING PAREN -->
                <xsl:when test="contains($string, '(')">
                    <subfield code="a">
                        <xsl:value-of select="substring-before($string, '(')" />
                    </subfield>
                </xsl:when>
                <!-- IF A DASH, CHECK IF IT'S A DATE OR PART OF THE NAME -->
                <xsl:when test="contains($string, '-')">
                    <xsl:variable name="name_1" select="substring-before($string, '-')" />
                    <xsl:choose>
                        <!-- IF IT'S A DATE REMOVE IT -->
                        <xsl:when test="translate(substring($name_1, (string-length($name_1)), 1), '0123456789', '9999999999') = '9'">
                            <xsl:variable name="name" select="substring($name_1, 1, (string-length($name_1)-6))" />
                            <subfield code="a">
                                <xsl:value-of select="$name" />
                            </subfield>
                        </xsl:when>
                        <!-- IF IT'S NOT A DATE, CHECK WHETHER THERE IS A DATE LATER -->
                        <xsl:otherwise>
                            <xsl:variable name="remainder" select="substring-after($string, '-')" />
                            <xsl:choose>
                                <!-- IF THERE'S A DASH, ASSUME IT'S A DATE AND REMOVE IT -->
                                <xsl:when test="contains($remainder, '-')">
                                    <xsl:variable name="tmp" select="substring-before($remainder, '-')" />
                                    <xsl:variable name="name_2" select="substring($tmp, 1, (string-length($tmp)-6))" />
                                    <subfield code="a">
                                        <xsl:value-of select="$name_1" />-<xsl:value-of select="$name_2" />
                                    </subfield>
                                </xsl:when>
                                <!-- IF THERE'S NO DASH IN THE REMAINDER, OUTPUT IT -->
                                <xsl:otherwise>
                                    <subfield code="a">
                                        <xsl:value-of select="$string" />
                                    </subfield>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <!-- NO DASHES, NO PARENS, JUST OUTPUT THE NAME -->
                <xsl:otherwise>
                    <subfield code="a">
                        <xsl:value-of select="$string" />
                        <xsl:text>,</xsl:text>
                    </subfield>
                </xsl:otherwise>
            </xsl:choose>

            <!-- CAPTURE SECONDARY NAME IN PARENS FOR SUBFIELD Q -->
            <xsl:if test="contains($string, '(')">
                <xsl:variable name="subq_tmp" select="substring-after($string, '(')" />
                <xsl:variable name="subq" select="substring-before($subq_tmp, ')')" />
                <subfield code="q">
                    <xsl:text>(</xsl:text>
                    <xsl:value-of select="$subq" />
                    <xsl:text>)</xsl:text>
                </subfield>
            </xsl:if>

            <!-- CAPTURE DATE FOR SUBFIELD D, ASSUME DATE IS LAST ITEM IN FIELD -->
            <!-- Note: does not work if name has a dash in it -->
            <xsl:if test="contains($string, '-')">
                <xsl:variable name="date_tmp" select="substring-before($string, '-')" />
                <xsl:variable name="remainder" select="substring-after($string, '-')" />
                <xsl:choose>
                    <!-- CHECK SECOND HALF FOR ANOTHER DASH; IF PRESENT, ASSUME THAT IS DATE -->
                    <xsl:when test="contains($remainder, '-')">
                        <xsl:variable name="tmp" select="substring-before($remainder, '-')" />
                        <xsl:variable name="date_1" select="substring($remainder, (string-length($tmp)-3))" />
                        <!-- CHECK WHETHER IT HAS A NUMBER BEFORE IT AND IF SO, OUTPUT IT AS DATE -->
                        <xsl:if test="translate(substring($date_1, 1, 1), '0123456789', '9999999999') = '9'">
                            <subfield code="d">
                                <xsl:value-of select="$date_1" />.
                            </subfield>
                        </xsl:if>
                    </xsl:when>
                    <!-- OTHERWISE THIS IS THE ONLY DASH SO TAKE IT -->
                    <xsl:otherwise>
                        <xsl:variable name="date_2" select="substring($string, (string-length($date_tmp)-3))" />
                        <!-- CHECK WHETHER IT HAS A NUMBER BEFORE IT AND IF SO, OUTPUT IT AS DATE -->
                        <xsl:if test="translate(substring($date_2, 1, 1), '0123456789', '9999999999') = '9'">
                            <subfield code="d">
                                <xsl:value-of select="$date_2" />.
                            </subfield>
                        </xsl:if>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:if>
            <subfield code="e">
                <xsl:value-of select="$type" />.
            </subfield>
            <xsl:if test="$orcid">
                <subfield code="0">
                    <xsl:text xml:space="preserve">(orcid)</xsl:text>
                    <xsl:value-of select="$orcid"/>
                </subfield>
            </xsl:if>
        </datafield>
    </xsl:template>


</xsl:stylesheet>
