<?xml version="1.0"?>
<xsl:stylesheet version="2.0"
                xmlns:date="http://exslt.org/dates-and-times"
                extension-element-prefixes="date"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="html" indent="yes" version="4.0"/>

    <xsl:key name="class" match="difference" use="@class"/>

    <xsl:template match="/">
        <xsl:text disable-output-escaping="yes">&lt;!DOCTYPE html&gt;</xsl:text>
        <xsl:element name="html">
            <xsl:attribute name="lang">en</xsl:attribute>
            <xsl:element name="head">
                <xsl:element name="meta">
                    <xsl:attribute name="charset">utf-8</xsl:attribute>
                </xsl:element>
                <xsl:element name="title">Binary Compatibility Report</xsl:element>
                <xsl:element name="style">
                    <xsl:text disable-output-escaping="yes">
                        body{margin:0;padding:0;font-family:sans-serif;font-size:12pt;}
                        body,a,a:visited{color:#303030;}
                        #content{padding-left:50px;padding-right:50px;padding-top:30px;padding-bottom:30px;}
                        #content h1{font-size:160%;margin-bottom:10px;}
                        #footer{margin-top:100px;font-size:80%;white-space:nowrap;}
                        #footer,#footer a{color:#a0a0a0;}
                        ul{margin-left:0;}
                        h1,h2,h3{white-space:nowrap;}
                        h2{font-size:120%;}
                        div.selected{display:block;}
                        div.deselected{display:none;}
                        #maintable{width:100%;border-collapse:collapse;}
                        #maintable th,#maintable td{border-bottom:solid #d0d0d0 1px;}
                        #maintable td{vertical-align:top}
                        th{text-align:left;white-space:nowrap;padding-left:2em;}
                        th:first-child{padding-left:0;}
                        td{padding-left:2em;padding-top:5px;padding-bottom:5px;}
                        td:first-child{padding-left:0;width:30%}
                        td.numeric,th.numeric{text-align:right;}
                        span.code{display:inline-block;margin-top:0em;margin-bottom:1em;}
                        span.code pre{font-size:11pt;padding-top:10px;padding-bottom:10px;padding-left:10px;padding-right:10px;margin:0;background-color:#f7f7f7;border:solid 1px #d0d0d0;min-width:700px;width:auto !important;width:700px;}
                        ul{margin:0px;padding:0px;}
                        .warning,.warning a{color:#fbcc45;}
                        .error,.error a{color:#b60808;}
                        .info, .info a{color:#3879d9}
                        #summary {margin-top: 30px;margin-bottom: 40px;border: solid 2px #d0d0d0;width:400px}
                        #summary table{border:none;}
                        #summary td{vertical-align:top;width:110px;padding-top:15px;padding-bottom:15px;text-align:center;}
                        #summary td p{margin:0;}
                    </xsl:text>
                </xsl:element>
                <xsl:element name="body">
                    <xsl:element name="div">
                        <xsl:attribute name="id">content</xsl:attribute>
                        <xsl:element name="h1">Binary Compatibility Report</xsl:element>
                        <xsl:element name="div">
                            <xsl:attribute name="id">summary</xsl:attribute>
                            <table>
                                <tr>
                                    <td>
                                        <p class="error">ERROR</p>
                                        <div>
                                            <xsl:value-of select="count(//difference[@binseverity = 'ERROR'])"/>
                                        </div>
                                    </td>
                                    <td>
                                        <p class="warning">WARNING</p>
                                        <div>
                                            <xsl:value-of select="count(//difference[@binseverity = 'WARNING'])"/>
                                        </div>
                                    </td>
                                    <td>
                                        <p class="info">INFO</p>
                                        <div>
                                            <xsl:value-of select="count(//difference[@binseverity = 'INFO'])"/>
                                        </div>
                                    </td>
                                </tr>
                            </table>
                        </xsl:element>
                        <xsl:element name="table">
                            <xsl:attribute name="id">maintable</xsl:attribute>
                            <xsl:element name="thead">
                                <xsl:element name="tr">
                                    <xsl:element name="th">Class</xsl:element>
                                    <xsl:element name="th">Results</xsl:element>
                                </xsl:element>
                            </xsl:element>
                            <xsl:element name="tbody">
                                <xsl:apply-templates
                                        select="diffreport/difference[generate-id(.)=generate-id(key('class',@class)[1])]"/>
                            </xsl:element>
                        </xsl:element>

                        <xsl:element name="footer">
                            <xsl:element name="p">
                                <xsl:text disable-output-escaping="yes">Generated by &lt;a href="http://clirr.sourceforge.net"&gt;Clirr&lt;/a&gt; at </xsl:text>
                                <xsl:value-of select="date:format-date(date:date-time(),'MMM d, yyyy HH:mm:ss Z')"/>
                            </xsl:element>
                        </xsl:element>
                    </xsl:element>
                </xsl:element>
            </xsl:element>
        </xsl:element>
    </xsl:template>

    <xsl:template match="difference">
        <xsl:element name="tr">
            <xsl:element name="td">
                <xsl:value-of select="@class"/>
            </xsl:element>
            <xsl:element name="td">
                <xsl:element name="ul">
                    <xsl:for-each select="key('class', @class)">
                        <xsl:element name="li">
                            <xsl:element name="span">
                                <xsl:attribute name="class">
                                    <xsl:value-of
                                            select="translate(@binseverity, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
                                </xsl:attribute>
                                <xsl:value-of select="@binseverity"/>
                                <xsl:text>: </xsl:text>
                            </xsl:element>
                            <xsl:value-of select="."/>
                        </xsl:element>
                    </xsl:for-each>
                </xsl:element>
                <!--<xsl:element name="table">-->

                <!--<xsl:element name="tr">-->
                <!--<xsl:element name="td">-->
                <!--<xsl:attribute name="class">-->
                <!--<xsl:value-of-->
                <!--select="translate(@srcseverity, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>-->
                <!--</xsl:attribute>-->
                <!--<xsl:value-of select="@srcseverity"/>-->
                <!--</xsl:element>-->
                <!--<xsl:element name="td">-->
                <!--<xsl:value-of select="@method"/>-->
                <!--</xsl:element>-->
                <!--<xsl:element name="td">-->
                <!--<xsl:value-of select="@field"/>-->
                <!--</xsl:element>-->
                <!--<xsl:element name="td">-->
                <!--<xsl:value-of select="."/>-->
                <!--</xsl:element>-->
                <!--</xsl:element>-->

                <!--</xsl:for-each>-->
                <!--</xsl:element>-->
            </xsl:element>
        </xsl:element>
    </xsl:template>


</xsl:stylesheet>