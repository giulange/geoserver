/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.raster;

import java.awt.Point;
import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import javax.media.jai.JAI;
import javax.media.jai.ParameterBlockJAI;
import javax.media.jai.RenderedOp;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.config.CoverageAccessInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.process.raster.CoverageUtilities;
import org.geotools.process.raster.changematrix.ChangeMatrixDescriptor.ChangeMatrix;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.resources.image.ImageUtilities;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.filter.Filter;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.parameter.GeneralParameterDescriptor;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A process that returns a coverage fully (something which is un-necessarily hard in WCS)
 * 
 * @author Simone Giannecchini, GeoSolutions SAS
 * @author Andrea Aime, GeoSolutions SAS
 */
@SuppressWarnings("deprecation")
@DescribeProcess(title = "ChangeMatrix", description = "Compute the ChangeMatrix between two coverages")
public class ChangeMatrixProcess implements GSProcess {

    private Catalog catalog;
	private GeoServer geoserver;

    public ChangeMatrixProcess(Catalog catalog, GeoServer geoserver) {
        this.catalog = catalog;
        this.geoserver= geoserver;
    }

    /**
     * @param classes representing the domain of the classes (Mandatory, not empty)
     * @param rasterT0 that is the reference Image (Mandatory)
     * @param rasterT1 rasterT1 that is the update situation (Mandatory)
     * @param roi that identifies the optional ROI (so that could be null)
     * @return
     */
    @DescribeResult(name = "changeMatrix", description = "the ChangeMatrix", type=ChangeMatrixDTO.class)
    public ChangeMatrixDTO execute(
            @DescribeParameter(name = "referenceName", description = "Name of reference raster, optionally fully qualified (workspace:name)") String referenceName,
            @DescribeParameter(name = "referenceFilter", description = "Filter to use on the raster data", min = 1) Filter referenceFilter,
            @DescribeParameter(name = "nowName", description = "Name of reference raster, optionally fully qualified (workspace:name)") String nowName,
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 1) Filter nowFilter,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi)
            throws IOException {
    	
    	// get the original Coverages
        CoverageInfo ciReference = catalog.getCoverageByName(referenceName);
        if (ciReference == null) {
            throw new WPSException("Could not find coverage " + referenceName);
        }
        CoverageInfo ciNow = catalog.getCoverageByName(nowName);
        if (ciNow == null) {
            throw new WPSException("Could not find coverage " + nowName);
        }

        RenderedOp result=null;
        GridCoverage2D nowCoverage=null;
        GridCoverage2D referenceCoverage=null;
        try{
        	
        // read reference coverage
        GridCoverageReader referenceReader = ciReference.getGridCoverageReader(null, null);
        ParameterValueGroup readParametersDescriptor = referenceReader.getFormat().getReadParameters();
        List<GeneralParameterDescriptor> parameterDescriptors = readParametersDescriptor.getDescriptor().descriptors();
        // get params for this coverage and override what's needed
        Map<String, Serializable> defaultParams = ciNow.getParameters();
        GeneralParameterValue[]params=CoverageUtils.getParameters(readParametersDescriptor, defaultParams, false);
        // merge filter
        params = replaceParameter(
        		params, 
        		referenceFilter, 
        		ImageMosaicFormat.FILTER);
        // merge USE_JAI_IMAGEREAD to false if needed
        params = replaceParameter(
        		params, 
        		ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(), 
        		ImageMosaicFormat.USE_JAI_IMAGEREAD);
        // TODO add tiling, reuse standard values from config
        // TODO add background value, reuse standard values from config
        referenceCoverage = (GridCoverage2D) referenceReader.read(params);
        
        
        // read now coverage
        GridCoverageReader nowReader = ciNow.getGridCoverageReader(null, null);
        readParametersDescriptor = nowReader.getFormat().getReadParameters();
        parameterDescriptors = readParametersDescriptor
                .getDescriptor().descriptors();
        // get params for this coverage and override what's needed
        defaultParams = ciNow.getParameters();
        params=CoverageUtils.getParameters(readParametersDescriptor, defaultParams, false);
        
        // merge filter
        params = CoverageUtils.mergeParameter(parameterDescriptors, params, nowFilter, "FILTER",
                    "Filter");
        // merge USE_JAI_IMAGEREAD to false if needed
        params = CoverageUtils.mergeParameter(
        		parameterDescriptors, 
        		params, 
        		ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(), 
        		ImageMosaicFormat.USE_JAI_IMAGEREAD.getName().toString(),
        		"USE_JAI_IMAGEREAD");
        // TODO add tiling, reuse standard values from config
        // TODO add background value, reuse standard values from config
        nowCoverage = (GridCoverage2D) nowReader.read(params);
        
        
        // now perform the operation
        final ChangeMatrix cm = new ChangeMatrix(classes);
        final ParameterBlockJAI pbj = new ParameterBlockJAI("ChangeMatrix");
        pbj.addSource(referenceCoverage.getRenderedImage());
        pbj.addSource(nowCoverage.getRenderedImage());
        pbj.setParameter("result", cm);
        // TODO handle Region Of Interest
        if(roi!=null){

            //
            // GRID TO WORLD preparation from reference
            //
            final AffineTransform mt2D = (AffineTransform) referenceCoverage.getGridGeometry().getGridToCRS2D(PixelOrientation.UPPER_LEFT);
            
            
            // check if we need to reproject the ROI from WGS84 (standard in the input) to the reference CRS
            final CoordinateReferenceSystem crs=referenceCoverage.getCoordinateReferenceSystem();
            if(CRS.equalsIgnoreMetadata(crs, DefaultGeographicCRS.WGS84)){
            	pbj.setParameter("ROI", CoverageUtilities.prepareROI(roi, mt2D));
            } else {
            	// reproject 
            	MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84, crs,true);
            	if(transform.isIdentity()){
            		pbj.setParameter("ROI", CoverageUtilities.prepareROI(roi, mt2D));
            	} else {
            		pbj.setParameter("ROI", CoverageUtilities.prepareROI(JTS.transform(roi, transform), mt2D));
            	}
            }
        }
        result = JAI.create("ChangeMatrix", pbj, null);

        // result computation
        final int numTileX=result.getNumXTiles();
        final int numTileY=result.getNumYTiles();
        final int minTileX=result.getMinTileX();
        final int minTileY=result.getMinTileY();
        final List<Point> tiles = new ArrayList<Point>(numTileX * numTileY);
        for (int i = minTileX; i < minTileX+numTileX; i++) {
            for (int j = minTileY; j < minTileY+numTileY; j++) {
                tiles.add(new Point(i, j));
            }
        }
        final CountDownLatch sem = new CountDownLatch(tiles.size());
        // how many JAI tiles do we have?
        final CoverageAccessInfo coverageAccess = geoserver.getGlobal().getCoverageAccess();
        final ThreadPoolExecutor executor = coverageAccess.getThreadPoolExecutor();
        final RenderedOp temp=result;
        for (final Point tile : tiles) {
        	
        	executor.execute(new Runnable() {

                @Override
                public void run() {
                	temp.getTile(tile.x, tile.y);
                    sem.countDown();
                }
            });
        }
        try {
			sem.await();
		} catch (InterruptedException e) {
			// TODO handle error
			return null;
		}        
		// computation done!
        cm.freeze();

        return new ChangeMatrixDTO(cm, classes);       

        }catch (Exception e) {
			// TODO: handle exception
		} finally{
	        // clean up
	        if(result!=null){
	        	ImageUtilities.disposePlanarImageChain(result);
	        }
	        if(referenceCoverage!=null){
	        	referenceCoverage.dispose(true);
	        }
	        if(nowCoverage!=null){
	        	nowCoverage.dispose(true);
	        }
		}
		
		// if we get here there something went wrong
		return null;
    }

    /**
     * Replace or add the provided parameter in the read parameters
     */
    private <T> GeneralParameterValue[] replaceParameter(
    		GeneralParameterValue[] readParameters, 
    		Object value, 
    		ParameterDescriptor<T> pd) {
        
        // scan all the params looking for the one we want to add
        for (GeneralParameterValue gpv : readParameters) {
            // in case of match of any alias add a param value to the lot
            if (gpv.getDescriptor().getName().equals(pd.getName())) {
                ((ParameterValue)gpv).setValue(value);
                // leave
                return readParameters;
            }
        }
        
        // add it to the array
        // add to the list
        GeneralParameterValue[] readParametersClone = new GeneralParameterValue[readParameters.length + 1];
        System.arraycopy(readParameters, 0, readParametersClone, 0,
                readParameters.length);
        final ParameterValue<T> pv=pd.createValue();
        pv.setValue(value);
        readParametersClone[readParameters.length] = pv;
        readParameters = readParametersClone;
        return readParameters;
    }
}