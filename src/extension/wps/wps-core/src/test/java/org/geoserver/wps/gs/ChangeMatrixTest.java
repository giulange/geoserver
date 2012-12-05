/* Copyright (c) 2012 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.geoserver.data.test.MockData;
import org.geoserver.data.test.SystemTestData;
import org.geoserver.wps.WPSTestSupport;
import org.geotools.gce.arcgrid.ArcGridFormat;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridEnvelope;

import com.mockrunner.mock.web.MockHttpServletResponse;

public class ChangeMatrixTest extends WPSTestSupport {
    
    @Override
    protected void onSetUp(SystemTestData testData) throws Exception {
        super.onSetUp(testData);
        
        addWcs11Coverages(testData);
    }

    @Test
    public void testChange() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+
        "<wps:Execute version=\"1.0.0\" service=\"WPS\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://www.opengis.net/wps/1.0.0\" xmlns:wfs=\"http://www.opengis.net/wfs\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:ogc=\"http://www.opengis.net/ogc\" xmlns:wcs=\"http://www.opengis.net/wcs/1.1.1\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd\">"+
          "<ows:Identifier>gs:ChangeMatrix</ows:Identifier>"+
          "<wps:DataInputs>"+
            "<wps:Input>"+
              "<ows:Identifier>referenceName</ows:Identifier>"+
              "<wps:Data>"+
                "<wps:LiteralData>corine</wps:LiteralData>"+
              "</wps:Data>"+
            "</wps:Input>"+
            "<wps:Input>"+
              "<ows:Identifier>referenceFilter</ows:Identifier>"+
              "<wps:Data>"+
                "<wps:ComplexData mimeType=\"text/plain; subtype=cql\"><![CDATA[year='2000']]></wps:ComplexData>"+
              "</wps:Data>"+
            "</wps:Input>"+
            "<wps:Input>"+
              "<ows:Identifier>nowName</ows:Identifier>"+
              "<wps:Data>"+
                "<wps:LiteralData>corine</wps:LiteralData>"+
              "</wps:Data>"+
            "</wps:Input>"+
            "<wps:Input>"+
              "<ows:Identifier>nowFilter</ows:Identifier>"+
              "<wps:Data>"+
                "<wps:ComplexData mimeType=\"text/plain; subtype=cql\"><![CDATA[year='2006']]></wps:ComplexData>"+
              "</wps:Data>"+
            "</wps:Input>"+
            "<wps:Input>"+
              "<ows:Identifier>classes</ows:Identifier>"+
              "<wps:Data>"+
                "<wps:LiteralData>1</wps:LiteralData>"+
              "</wps:Data>"+
            "</wps:Input>"+
            "<wps:Input>"+
            "<ows:Identifier>classes</ows:Identifier>"+
            "<wps:Data>"+
              "<wps:LiteralData>2</wps:LiteralData>"+
            "</wps:Data>"+
          "</wps:Input>"+
          "<wps:Input>"+
          "<ows:Identifier>classes</ows:Identifier>"+
          "<wps:Data>"+
            "<wps:LiteralData>5</wps:LiteralData>"+
          "</wps:Data>"+
        "</wps:Input>"+
        "<wps:Input>"+
        "<ows:Identifier>classes</ows:Identifier>"+
        "<wps:Data>"+
          "<wps:LiteralData>7</wps:LiteralData>"+
        "</wps:Data>"+
      "</wps:Input>"+
      "<wps:Input>"+
      "<ows:Identifier>classes</ows:Identifier>"+
      "<wps:Data>"+
        "<wps:LiteralData>9</wps:LiteralData>"+
      "</wps:Data>"+
    "</wps:Input>"+      
          "</wps:DataInputs>"+
          "<wps:ResponseForm>"+
            "<wps:RawDataOutput mimeType=\"application/json\">"+
              "<ows:Identifier>changeMatrix</ows:Identifier>"+
            "</wps:RawDataOutput>"+
          "</wps:ResponseForm>"+
        "</wps:Execute>";

        MockHttpServletResponse response = postAsServletResponse(root(), xml);
        // System.out.println(response.getOutputStreamContent());
        InputStream is = getBinaryInputStream(response);
        
        IOUtils.copy(is, System.out);
        
    }
}
