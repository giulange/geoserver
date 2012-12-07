/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.raster;

import java.awt.Point;
import java.awt.Shape;
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
import javax.media.jai.ROI;
import javax.media.jai.ROIShape;
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
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.image.jai.Registry;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.process.raster.CoverageUtilities;
import org.geotools.process.raster.changematrix.ChangeMatrixDescriptor;
import org.geotools.process.raster.changematrix.ChangeMatrixDescriptor.ChangeMatrix;
import org.geotools.process.raster.changematrix.ChangeMatrixRIF;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.resources.image.ImageUtilities;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.filter.Filter;
import org.opengis.metadata.spatial.PixelOrientation;
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
@DescribeProcess(title = "ChangeMatrix", description = "Compute the ChangeMatrix between two coverages")
public class ChangeMatrixProcess implements GSProcess {
	
    static {
        Registry.registerRIF(JAI.getDefaultInstance(), new ChangeMatrixDescriptor(), new ChangeMatrixRIF(),
                Registry.JAI_TOOLS_PRODUCT);
    }
    
	private final static boolean DEBUG= Boolean.getBoolean("org.geoserver.wps.debug");

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
            @DescribeParameter(name = "name", description = "Name of the raster, optionally fully qualified (workspace:name)") String referenceName,
            @DescribeParameter(name = "referenceFilter", description = "Filter to use on the raster data", min = 1) Filter referenceFilter,
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 1) Filter nowFilter,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi)
            throws IOException {
    	
    	// DEBUG OPTION
    	if(DEBUG){
    		return getTestMap();
    	}
    	// get the original Coverages
        CoverageInfo ciReference = catalog.getCoverageByName(referenceName);
        if (ciReference == null) {
            throw new WPSException("Could not find coverage " + referenceName);
        }
        
        RenderedOp result=null;
        GridCoverage2D nowCoverage=null;
        GridCoverage2D referenceCoverage=null;
        try{
        	
        // read reference coverage
        GridCoverageReader referenceReader = ciReference.getGridCoverageReader(null, null);
        ParameterValueGroup readParametersDescriptor = referenceReader.getFormat().getReadParameters();
        // get params for this coverage and override what's needed
        Map<String, Serializable> defaultParams = ciReference.getParameters();
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
        readParametersDescriptor = referenceReader.getFormat().getReadParameters();
        // get params for this coverage and override what's needed
        defaultParams = ciReference.getParameters();
        params=CoverageUtils.getParameters(readParametersDescriptor, defaultParams, false);
        
        // merge filter
        // merge filter
        params = replaceParameter(
        		params, 
        		nowFilter, 
        		ImageMosaicFormat.FILTER);
        // merge USE_JAI_IMAGEREAD to false if needed
        params = replaceParameter(
        		params, 
        		ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(), 
        		ImageMosaicFormat.USE_JAI_IMAGEREAD);
        // TODO add tiling, reuse standard values from config
        // TODO add background value, reuse standard values from config
        nowCoverage = (GridCoverage2D) referenceReader.read(params);
        
        
        // now perform the operation
        final ChangeMatrix cm = new ChangeMatrix(classes);
        final ParameterBlockJAI pbj = new ParameterBlockJAI("ChangeMatrix");
        pbj.addSource(referenceCoverage.getRenderedImage());
        pbj.addSource(nowCoverage.getRenderedImage());
        pbj.setParameter("result", cm);
        // handle Region Of Interest
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
            		pbj.setParameter("ROI", prepareROI(roi, mt2D));
            	} else {
            		pbj.setParameter("ROI", prepareROI(JTS.transform(roi, transform), mt2D));
            	}
            }
        }
        result = JAI.create("ChangeMatrix", pbj, null);

        //
        // result computation
        //
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
        	throw new WPSException("Could process request ",e);
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
    }


    /**
     * Transform the provided {@link Geometry} in world coordinates into
     * 
     * @param roi
     * @param gridToWorld
     * @return
     * @throws Exception
     */
    private static ROI prepareROI(Geometry roi, AffineTransform gridToWorld) throws Exception {
        final Shape cropRoiLS2 = new LiteShape2(roi, ProjectiveTransform.create(gridToWorld)
                .inverse(), null, true, 1);
        return new ROIShape(cropRoiLS2);
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
    
    /**
    * @return an hardcoded ChangeMatrixOutput usefull for testing
    */
        private static final ChangeMatrixDTO getTestMap() {

            ChangeMatrixDTO s = new ChangeMatrixDTO();

            s.add(new ChangeMatrixElement(0, 0, 16002481));
            s.add(new ChangeMatrixElement(0, 35, 0));
            s.add(new ChangeMatrixElement(0, 1, 0));
            s.add(new ChangeMatrixElement(0, 36, 4));
            s.add(new ChangeMatrixElement(0, 37, 4));

            s.add(new ChangeMatrixElement(1, 0, 0));
            s.add(new ChangeMatrixElement(1, 35, 0));
            s.add(new ChangeMatrixElement(1, 1, 3192));
            s.add(new ChangeMatrixElement(1, 36, 15));
            s.add(new ChangeMatrixElement(1, 37, 0));

            s.add(new ChangeMatrixElement(35, 0, 0));
            s.add(new ChangeMatrixElement(35, 35, 7546));
            s.add(new ChangeMatrixElement(35, 1, 0));
            s.add(new ChangeMatrixElement(35, 36, 0));
            s.add(new ChangeMatrixElement(35, 37, 16));

            s.add(new ChangeMatrixElement(36, 0, 166));
            s.add(new ChangeMatrixElement(36, 35, 36));
            s.add(new ChangeMatrixElement(36, 1, 117));
            s.add(new ChangeMatrixElement(36, 36, 1273887));
            s.add(new ChangeMatrixElement(36, 37, 11976));

            s.add(new ChangeMatrixElement(37, 0, 274));
            s.add(new ChangeMatrixElement(37, 35, 16));
            s.add(new ChangeMatrixElement(37, 1, 16));
            s.add(new ChangeMatrixElement(37, 36, 28710));
            s.add(new ChangeMatrixElement(37, 37, 346154));

            return s;
        }    
}