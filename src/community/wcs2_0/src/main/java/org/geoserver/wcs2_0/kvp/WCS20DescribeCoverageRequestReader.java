/* Copyright (c) 2012 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wcs2_0.kvp;

import net.opengis.wcs20.DescribeCoverageType;

import net.opengis.wcs20.Wcs20Factory;

import org.geoserver.ows.kvp.EMFKvpRequestReader;

/**
 * Parses a DescribeCoverage request for WCS into the correspondent model object
 * 
 * @author Emanuele Tajariol (etj) - GeoSolutions
 * 
 */
public class WCS20DescribeCoverageRequestReader extends EMFKvpRequestReader {

    public WCS20DescribeCoverageRequestReader() {
        super(DescribeCoverageType.class, Wcs20Factory.eINSTANCE);
    }
}