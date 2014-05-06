/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.raster;

import static jcuda.driver.CUdevice_attribute.CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK;
import static jcuda.driver.JCudaDriver.cuCtxCreate;
import static jcuda.driver.JCudaDriver.cuCtxDestroy;
import static jcuda.driver.JCudaDriver.cuCtxSynchronize;
import static jcuda.driver.JCudaDriver.cuDeviceGet;
import static jcuda.driver.JCudaDriver.cuDeviceGetAttribute;
import static jcuda.driver.JCudaDriver.cuDeviceGetCount;
import static jcuda.driver.JCudaDriver.cuInit;
import static jcuda.driver.JCudaDriver.cuLaunchKernel;
import static jcuda.driver.JCudaDriver.cuMemAlloc;
import static jcuda.driver.JCudaDriver.cuMemFree;
import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;
import static jcuda.driver.JCudaDriver.cuMemcpyHtoD;
import static jcuda.driver.JCudaDriver.cuModuleGetFunction;
import static jcuda.driver.JCudaDriver.cuModuleLoad;
import static jcuda.driver.JCudaDriver.cuModuleUnload;
import static jcuda.runtime.JCuda.cudaMemset;
import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUcontext;
import jcuda.driver.CUdevice;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;
import jcuda.driver.JCudaDriver;

import it.geosolutions.jaiext.changematrix.ChangeMatrixDescriptor;
import it.geosolutions.jaiext.changematrix.ChangeMatrixDescriptor.ChangeMatrix;
import it.geosolutions.jaiext.changematrix.ChangeMatrixRIF;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferInt;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import javax.media.jai.BorderExtender;
import javax.media.jai.ImageLayout;
import javax.media.jai.JAI;
import javax.media.jai.ParameterBlockJAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.ROI;
import javax.media.jai.ROIShape;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;

import net.sf.json.JSONSerializer;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.gdal.gdalconst.gdalconstConstants;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.config.CoverageAccessInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.gs.ImportProcess;
import org.geoserver.wps.gs.ToFeature;
import org.geoserver.wps.gs.WFSLog;
import org.geoserver.wps.ppio.FeatureAttribute;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataSourceException;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.image.crop.GTCropDescriptor;
import org.geotools.image.jai.Registry;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.process.raster.CoverageUtilities;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderedimage.viewer.RenderedImageBrowser;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.NullProgressListener;
import org.geotools.utils.imageoverviews.OverviewsEmbedder;
import org.geotools.utils.progress.ExceptionEvent;
import org.geotools.utils.progress.ProcessingEvent;
import org.geotools.utils.progress.ProcessingEventListener;
import org.jaitools.imageutils.ROIGeometry;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.vfny.geoserver.global.GeoserverDataDirectory;

import com.sun.media.imageioimpl.common.ImageUtil;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * A process that returns a coverage fully (something which is un-necessarily hard in WCS)
 * 
 * @author Simone Giannecchini, GeoSolutions SAS
 * @author Andrea Aime, GeoSolutions SAS
 */
@DescribeProcess(title = "cudaChangeMatrix", description = "Compute the ChangeMatrix between two coverages")
public class CudaChangeMatrixProcess implements GSProcess {


    private final static boolean DEBUG = Boolean.getBoolean("org.geoserver.wps.debug");

    private static final int PIXEL_MULTY_ARG_INDEX = 100;
    
    // Create the PTX file by calling the NVCC
    private final static String PTX_FILE_NAME = "/opt/soil_sealing/cudacodes/changemat.ptx";

    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(
            new PrecisionModel());

    private Catalog catalog;

    private GeoServer geoserver;

    public CudaChangeMatrixProcess(Catalog catalog, GeoServer geoserver) {
        this.catalog = catalog;
        this.geoserver = geoserver;
    }

    /**
     * @param classes representing the domain of the classes (Mandatory, not empty)
     * @param rasterT0 that is the reference Image (Mandatory)
     * @param rasterT1 rasterT1 that is the update situation (Mandatory)
     * @param roi that identifies the optional ROI (so that could be null)
     * @return
     */
    @DescribeResult(name = "cudaChangeMatrix", description = "the ChangeMatrix", type = ChangeMatrixDTO.class)
    public ChangeMatrixDTO execute(
            @DescribeParameter(name = "name", description = "Name of the raster, optionally fully qualified (workspace:name)") String referenceName,
            @DescribeParameter(name = "defaultStyle", description = "Name of the raster default style") String defaultStyle,
            @DescribeParameter(name = "storeName", description = "Name of the destination data store to log info") String storeName,
            @DescribeParameter(name = "typeName", description = "Name of the destination feature type to log info") String typeName,
            @DescribeParameter(name = "referenceFilter", description = "Filter to use on the raster data", min = 1) Filter referenceFilter,
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 1) Filter nowFilter,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi)
            throws IOException {

        // DEBUG OPTION
        if (DEBUG) {
            return getTestMap();
        }

        // get the original Coverages
        CoverageInfo ciReference = catalog.getCoverageByName(referenceName);
        if (ciReference == null) {
            throw new WPSException("Could not find coverage " + referenceName);
        }

        // ///////////////////////////////////////////////
        // ChangeMatrix outcome variables ...
        RenderedOp result = null;
        GridCoverage2D nowCoverage = null;
        GridCoverage2D referenceCoverage = null;
        // ///////////////////////////////////////////////

        // ///////////////////////////////////////////////
        // Logging to WFS variables ...
        final String wsName = ciReference.getNamespace().getPrefix();
        final UUID uuid = UUID.randomUUID();
        SimpleFeatureCollection features = null;
        Filter filter = null;
        ToFeature toFeatureProcess = new ToFeature();
        WFSLog wfsLogProcess = new WFSLog(geoserver);
        // ///////////////////////////////////////////////

        try {

            // read reference coverage
            GridCoverageReader referenceReader = ciReference.getGridCoverageReader(null, null);
            ParameterValueGroup readParametersDescriptor = referenceReader.getFormat()
                    .getReadParameters();

            // get params for this coverage and override what's needed
            Map<String, Serializable> defaultParams = ciReference.getParameters();
            GeneralParameterValue[] params = CoverageUtils.getParameters(readParametersDescriptor,
                    defaultParams, false);

            GridGeometry2D gridROI = null;

            // Geometry associated with the ROI
            Geometry roiPrj = null;
         // GRID TO WORLD preparation from reference
            AffineTransform gridToWorldCornerROI=null; 
            
            // handle Region Of Interest
            if (roi != null) {
                if (roi instanceof GeometryCollection) {
                    List<Polygon> geomPolys = new ArrayList<Polygon>();
                    for (int g = 0; g < ((GeometryCollection) roi).getNumGeometries(); g++) {
                        extractPolygons(geomPolys, ((GeometryCollection) roi).getGeometryN(g));
                    }

                    if (geomPolys.size() == 0) {
                        roi = GEOMETRY_FACTORY.createPolygon(null, null);
                    } else if (geomPolys.size() == 1) {
                        roi = geomPolys.get(0);
                    } else {
                        roi = roi.getFactory().createMultiPolygon(
                                geomPolys.toArray(new Polygon[geomPolys.size()]));
                    }
                }
                //
                // Make sure the provided roi intersects the layer BBOX in wgs84 
                //
                final ReferencedEnvelope wgs84BBOX=ciReference.getLatLonBoundingBox();
                roi=roi.intersection(JTS.toGeometry(wgs84BBOX));
                if(roi.isEmpty()){
                	throw new WPSException("The provided ROI does not intersect the reference data BBOX: ",roi.toText());
                }
                
                //
                // GRID TO WORLD preparation from reference
                //
                gridToWorldCornerROI = (AffineTransform) ((GridGeometry2D) ciReference
                        .getGrid()).getGridToCRS2D(PixelOrientation.UPPER_LEFT);

                // check if we need to reproject the ROI from WGS84 (standard in the input) to the reference CRS
                final CoordinateReferenceSystem crs = ciReference.getCRS();
                if (CRS.equalsIgnoreMetadata(crs, DefaultGeographicCRS.WGS84)) {
                    roiPrj = roi;
                    //pbj.setParameter("ROI", CoverageUtilities.prepareROI(roi, gridToWorldCorner));
                } else {
                    // reproject
                    MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84,
                            crs, true);
                    if (transform.isIdentity()) {
                        roiPrj = roi;
                    } else {
                        roiPrj = JTS.transform(roi, transform);
                    }
                    //pbj.setParameter("ROI", prepareROIGeometry(roiPrj, gridToWorldCorner));
                }
                
                //
                // Make sure the provided area intersects the layer BBOX in the layer CRS
                //
                final ReferencedEnvelope crsBBOX = ciReference.boundingBox();
                roiPrj = roiPrj.intersection(JTS.toGeometry(crsBBOX));
                if (roiPrj.isEmpty()) {
                    throw new WPSException(
                            "The provided ROI does not intersect the reference data BBOX: ",
                            roiPrj.toText());
                }
                
                // Creation of a GridGeometry object used for forcing the reader
                Envelope envelope = roiPrj.getEnvelopeInternal();
                // create with supplied crs
                Envelope2D bounds = JTS.getEnvelope2D(envelope, crs);
                // Creation of a GridGeometry2D instance used for cropping the input images
                gridROI = new GridGeometry2D(PixelInCell.CELL_CORNER,
                        (MathTransform) gridToWorldCornerROI, bounds, null);
            }

            // merge filter
            params = replaceParameter(params, referenceFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = replaceParameter(params,
                    false,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);
            if (gridROI != null) {
                params = replaceParameter(params, gridROI, AbstractGridFormat.READ_GRIDGEOMETRY2D);
            }
            referenceCoverage = (GridCoverage2D) referenceReader.read(params);

            if (referenceCoverage == null) {
                throw new WPSException("Input Reference Coverage not found");
            }
            
            // read now coverage
            readParametersDescriptor = referenceReader.getFormat().getReadParameters();
            // get params for this coverage and override what's needed
            defaultParams = ciReference.getParameters();
            params = CoverageUtils.getParameters(readParametersDescriptor, defaultParams, false);

            // merge filter
            params = replaceParameter(params, nowFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = replaceParameter(params,
                    false,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);
            if (gridROI != null) {
                params = replaceParameter(params, gridROI, AbstractGridFormat.READ_GRIDGEOMETRY2D);
            }
            // TODO add tiling, reuse standard values from config
            // TODO add background value, reuse standard values from config
            nowCoverage = (GridCoverage2D) referenceReader.read(params);

            if (nowCoverage == null) {
                throw new WPSException("Input Current Coverage not found");
            }
            
            // Setting of the sources
            //pbj.addSource(referenceCoverage.getRenderedImage());
            //pbj.addSource(nowCoverage.getRenderedImage());
            // Image creation
            RenderedImage refImage = referenceCoverage.getRenderedImage();
            RenderedImage curImage = nowCoverage.getRenderedImage();

//            GeoTiffWriter writer = new GeoTiffWriter(new File("/home/giuliano/git/jai-ext/out/reference.tif"));
//            writer.write(referenceCoverage, null); 
//            writer.dispose();
//            
//            writer = new GeoTiffWriter(new File("/home/giuliano/git/jai-ext/out/now.tif"));
//            writer.write(nowCoverage, null); 
//            writer.dispose();
            
            
            
            //Image reference
            //RenderedImageBrowser.showChain(refImage);
            
            //System.out.println();
            
            //Image reference
            //RenderedImageBrowser.showChain(curImage);
            
            //System.out.println();
            
            
            // //////////////////////////////////////////////////////////////////////
            // Logging to WFS ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * Convert the spread attributes into a FeatureType
             */
            List<FeatureAttribute> attributes = new ArrayList<FeatureAttribute>();

            attributes.add(new FeatureAttribute("ftUUID", uuid.toString()));
            attributes.add(new FeatureAttribute("runBegin", new Date()));
            attributes.add(new FeatureAttribute("runEnd", new Date()));
            attributes.add(new FeatureAttribute("itemStatus", "RUNNING"));
            attributes.add(new FeatureAttribute("itemStatusMessage", "Instrumented by Server"));
            attributes.add(new FeatureAttribute("referenceName", referenceName));
            attributes.add(new FeatureAttribute("defaultStyle", defaultStyle));
            attributes.add(new FeatureAttribute("referenceFilter", referenceFilter.toString()));
            attributes.add(new FeatureAttribute("nowFilter", nowFilter.toString()));
            attributes.add(new FeatureAttribute("roi", roi.getEnvelope()));
            attributes.add(new FeatureAttribute("wsName", wsName));
            attributes.add(new FeatureAttribute("layerName", ""));
            attributes.add(new FeatureAttribute("changeMatrix", ""));

            features = toFeatureProcess.execute(JTS.toGeometry(ciReference.getNativeBoundingBox()),
                    ciReference.getCRS(), typeName, attributes, null);

            if (features == null || features.isEmpty()) {
                throw new ProcessException(
                        "There was an error while converting attributes into FeatureType.");
            }

            /**
             * LOG into the DB
             */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));
            features = wfsLogProcess.execute(features, typeName, wsName, storeName, filter, true,
                    new NullProgressListener());

            if (features == null || features.isEmpty()) {
                throw new ProcessException(
                        "There was an error while logging FeatureType into the storage.");
            }

            // //////////////////////////////////////////////////////////////////////
            // Compute the Change Matrix ...
            // //////////////////////////////////////////////////////////////////////
            
            
            
            // resize cropped-image to fit CUDA blockSize:
            Rectangle rectIMG = new Rectangle(refImage.getMinX(), refImage.getMinY(), refImage.getWidth(), refImage.getHeight());
            int cudaBlkSizX = 128; // I am assuming a squared blockDim
            int finalW = refImage.getWidth();
            int finalH = refImage.getHeight();
            if(finalW%cudaBlkSizX != 0){
            	finalW = finalW + (cudaBlkSizX - finalW%cudaBlkSizX);
            }
            if(finalH%cudaBlkSizX != 0){
            	finalH = finalH + (cudaBlkSizX - finalH%cudaBlkSizX);
            }
            // end -- resize           
            
            BorderExtender extender = BorderExtender.createInstance(BorderExtender.BORDER_ZERO);
            Rectangle finalRect = new Rectangle(rectIMG.x, rectIMG.y, finalW, finalH);
            
            PlanarImage refFinal = PlanarImage.wrapRenderedImage(refImage);
            ImageUtilities.disposePlanarImageChain(PlanarImage.wrapRenderedImage(refImage));
            
            Raster ref = refFinal.getExtendedData(finalRect, extender);
            // Dispose first image for reducing memory occupation
            refFinal.dispose();
            
            
            PlanarImage curFinal = PlanarImage.wrapRenderedImage(curImage);
            ImageUtilities.disposePlanarImageChain(PlanarImage.wrapRenderedImage(curImage));

            Raster cur = curFinal.getExtendedData(finalRect, extender);
            // Dispose second image for reducing memory occupation
            curFinal.dispose();
            
            // transform into byte array
            final DataBufferByte dbRef = (DataBufferByte) ref.getDataBuffer();
            final DataBufferByte dbCurrent = (DataBufferByte) cur.getDataBuffer();
            byte dataRef[]=dbRef.getData(0);
            byte dataCurrent[]=dbCurrent.getData(0);
            
            if(dataRef.length!=finalRect.width*finalRect.height||dataCurrent.length!=finalRect.width*finalRect.height){
            	System.out.println("a");
            }
            
            // Here I must consider the effective number of classes + the NODATA value (which is expected to be ZERO for CUDA performance)
            int numClasses = classes.size()+1;
            
            //System.out.println("Calling JCUDA tileX:"+tileX+" tileY:"+tileY);//+" bbox:"+rect.toString());
            //System.out.println("Calling JCUDA tileW:"+rect.width+" tileH:"+rect.height);
            /* 		Call CUDA:
             * 			> iMaps:		must have ZERO as NODATA value.
             * 			> numclasses:	must count the effective number of classes + 1 (for nodata value).
             * 		and get results in List:
             * 			> host_oMap:	it is the first array.
             * 			> host_chMat:	it is the second array.
             */
            final List<int[]> resultCuda=JCudaChangeMat(dataRef,dataCurrent,numClasses, finalRect.width, finalRect.height, the_ROI_name);
            if(resultCuda.get(0).length!=finalRect.width*finalRect.height){
            	System.out.println("a");
            }
            
            
            // Image creation from data
            RenderedImage map = createImage(finalRect, resultCuda.get(0));
            
            // CROP
            // hints for tiling
            final Hints hints = GeoTools.getDefaultHints().clone();
            
            
            result = GTCropDescriptor.create(map, rectIMG.x*1.0f, rectIMG.y*1.0f, rectIMG.width*1.0f, rectIMG.height*1.0f, hints);
            ImageUtilities.disposePlanarImageChain(PlanarImage.wrapRenderedImage(map));
            
            final String rasterName = ciReference.getName() + "_cm_" + System.nanoTime();
            
            // Extract Changematrix
            /**
             * creating the ChangeMatrix grid
             */
            final ChangeMatrixDTO changeMatrix = new ChangeMatrixDTO();
            
            int[] changeMat =  resultCuda.get(1);
            
            for(int i = 0; i < numClasses; i++){
            	for(int j = 0; j < numClasses; j++){
            		// Value 
            		int classValue = changeMat[j + i*numClasses];
            		ChangeMatrixElement el = new ChangeMatrixElement(i, j, classValue);
            		changeMatrix.add(el);
                }
            }
            // Raster name setting
            changeMatrix.setRasterName(rasterName);
            
            // //////////////////////////////////////////////////////////////////////
            // Import into GeoServer the new raster 'result' ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * create the final coverage using final envelope
             */
            

            
            final GridCoverage2D retValue = new GridCoverageFactory(hints).create(rasterName,
                    result, referenceCoverage.getEnvelope());

            /**
             * import the new coverage into the GeoServer catalog
             */
            ImportProcess importProcess = new ImportProcess(catalog);
            importProcess.execute(null, retValue, wsName, null, retValue.getName().toString(),
                    retValue.getCoordinateReferenceSystem(), null, defaultStyle);

            /**
             * Add Overviews...
             */
            CoverageStoreInfo importedCoverageInfo = catalog.getCoverageStoreByName(rasterName);
            File tiffFile = GeoserverDataDirectory.findDataFile(importedCoverageInfo.getURL());
            if (tiffFile != null && tiffFile.exists() && tiffFile.isFile() && tiffFile.canWrite()) {
                try {
                    generateOverviews(importedCoverageInfo.getFormat().getReader(tiffFile));
                } catch (DataSourceException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

//            /**
//             * creating the ChangeMatrix grid
//             */
//            final ChangeMatrixDTO changeMatrix = new ChangeMatrixDTO(cm, classes, rasterName);

            // //////////////////////////////////////////////////////////////////////
            // Updating WFS ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * Update Feature Attributes and LOG into the DB
             */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter)
                    .toArray(new SimpleFeature[1])[0]);

            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "COMPLETED");
            feature.setAttribute("itemStatusMessage",
                    "Change Matrix Process completed successfully");
            feature.setAttribute("layerName", rasterName);
            feature.setAttribute("changeMatrix", JSONSerializer.toJSON(changeMatrix).toString());

            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);

            features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter, false,
                    new NullProgressListener());

            // //////////////////////////////////////////////////////////////////////
            // Return the computed Change Matrix ...
            // //////////////////////////////////////////////////////////////////////
            return changeMatrix;
        } catch (Exception e) {

            if (features != null) {
                // //////////////////////////////////////////////////////////////////////
                // Updating WFS ...
                // //////////////////////////////////////////////////////////////////////
                /**
                 * Update Feature Attributes and LOG into the DB
                 */
                filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

                SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter)
                        .toArray(new SimpleFeature[1])[0]);

                // build the feature
                feature.setAttribute("runEnd", new Date());
                feature.setAttribute("itemStatus", "FAILED");
                feature.setAttribute(
                        "itemStatusMessage",
                        "There was an error while while processing Input parameters: "
                                + e.getMessage());

                ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
                output.add(feature);

                features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter,
                        false, new NullProgressListener());
            }

            throw new WPSException("Could process request ", e);
        } finally {
            // clean up
            if (result != null) {
                ImageUtilities.disposePlanarImageChain(result);
            }
            if (referenceCoverage != null) {
                referenceCoverage.dispose(true);
            }
            if (nowCoverage != null) {
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
     * Transform the provided {@link Geometry} in world coordinates into
     * 
     * @param roi
     * @param gridToWorld
     * @return
     * @throws Exception
     */
    private static ROI prepareROIGeometry(Geometry roi, AffineTransform gridToWorld)
            throws Exception {

        Geometry projected = JTS.transform(roi, ProjectiveTransform.create(gridToWorld).inverse());

        return new ROIGeometry(projected);
    }

    /**
     * Replace or add the provided parameter in the read parameters
     */
    private <T> GeneralParameterValue[] replaceParameter(GeneralParameterValue[] readParameters,
            Object value, ParameterDescriptor<T> pd) {

        // scan all the params looking for the one we want to add
        for (GeneralParameterValue gpv : readParameters) {
            // in case of match of any alias add a param value to the lot
            if (gpv.getDescriptor().getName().equals(pd.getName())) {
                ((ParameterValue) gpv).setValue(value);
                // leave
                return readParameters;
            }
        }

        // add it to the array
        // add to the list
        GeneralParameterValue[] readParametersClone = new GeneralParameterValue[readParameters.length + 1];
        System.arraycopy(readParameters, 0, readParametersClone, 0, readParameters.length);
        final ParameterValue<T> pv = pd.createValue();
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

    /**
     * @param geomPolys
     * @param geom
     */
    public static void extractPolygons(Collection<Polygon> geomPolys, Geometry geom) {
        if (geom instanceof MultiPolygon) {
            for (int i = 0; i < ((MultiPolygon) geom).getNumGeometries(); i++) {
                Geometry g = ((MultiPolygon) geom).getGeometryN(i);
                if (g instanceof Polygon) {
                    if (g.getGeometryType().compareToIgnoreCase("Polygon") == 0) {
                        g.setSRID(geom.getSRID());
                        geomPolys.add((Polygon) g);
                    }
                }
            }
        } else if (geom instanceof Polygon) {
            if (geom.getGeometryType().compareToIgnoreCase("Polygon") == 0) {
                geomPolys.add((Polygon) geom);
            }
        }
    }

    /**
     * @param retValue
     * @return the number of steps processed, or 0 if none was done, or -1 on error.
     * @throws DataSourceException
     */
    public static int generateOverviews(AbstractGridCoverage2DReader abstractGridCoverage2DReader)
            throws DataSourceException {
        final File geotiffFile = (File) abstractGridCoverage2DReader.getSource();
        // ////
        // Adding Overviews
        // ////

        int tileH = 512;
        int tileW = 512;

        /** computing the number of steps **/
        GridEnvelope gridRange = abstractGridCoverage2DReader.getOriginalGridRange();

        int height = gridRange.getSpan(1);
        int width = gridRange.getSpan(0);

        int ratioH = (int) Math.ceil((1.0 * height) / tileH);
        int ratioW = (int) Math.ceil((1.0 * width) / tileW);

        int nStepsH = 0;
        int nStepsW = 0;

        if (ratioH >= 2) {
            nStepsH = (int) Math.floor(Math.log(ratioH) / Math.log(2));
        }

        if (ratioW >= 2) {
            nStepsW = (int) Math.floor(Math.log(ratioW) / Math.log(2));
        }

        int numSteps = Math.min(nStepsH, nStepsW);
        int downSampleSteps = 2;

        if (numSteps > 0) {
            final OverviewsEmbedder oe = new OverviewsEmbedder();
            oe.setDownsampleStep(downSampleSteps);
            oe.setNumSteps(numSteps);
            oe.setScaleAlgorithm(OverviewsEmbedder.SubsampleAlgorithm.Nearest.toString());
            oe.setTileCache(JAI.getDefaultInstance().getTileCache());
//            oe.setTileHeight(tileH);
//            oe.setTileWidth(tileW);
            oe.setSourcePath(geotiffFile.getAbsolutePath());

            EmbedderListener listener = new EmbedderListener(geotiffFile.getAbsolutePath());
            // add logger/listener
            oe.addProcessingEventListener(listener);

            // run
            oe.run(); // should block until terminated
            return listener.isSuccess() ? numSteps : -1;
        } else
            return 0;
    }

    static class EmbedderListener implements ProcessingEventListener {
        final String filename;

        boolean success = false;

        public EmbedderListener(String filename) {
            this.filename = filename;
        }

        public boolean isSuccess() {
            return success;
        }

        public void exceptionOccurred(ExceptionEvent event) {
            success = false;
        }

        public void getNotification(ProcessingEvent event) {
            if (event.getPercentage() == 100.0) {
                success = true;
            }
        }
    }
    
    /**
     * Stub method to be replaced with CUDA code
     * @param host_iMap1	reference map, 	nodata=0 [uint8]
     * @param host_iMap2	current map, 	nodata=0 [uint8]
     * @param host_roiMap	roi map, 1->to-be-counted & 0->ignoring pixels [uint8]
     * @param numclasses	number of classes including zero (which is NODATA value)
     * @param imWidth 		number of pixels of *iMap* along X
     * @param imHeight 		number of pixels of *iMap* along Y
     * @return a list of uint32 arrays containing (1) oMap and (2) changeMatrix
     */
    private List<int[]> JCudaChangeMat(byte[] host_iMap1,byte[] host_iMap2, byte[] host_roiMap, int numclasses,int imWidth, int imHeight)
	{
        /*
         * Copyright 2013 Massimo Nicolazzo & Giuliano Langella:
         * 	---- completed ----
         * 		(1) kernel-1	--->{oMap, tiled changeMat}
         * 		(2) kernel-2	--->{changeMat}
         */
        
        /**
         * This uses the JCuda driver bindings to load and execute two 
         * CUDA kernels:
         * (1)
         * The first kernel executes the change matrix computation for
         * the whole ROI given as input in the form of iMap1 & iMap2.
         * It returns a 3D change matrix in which every 2D array 
         * corresponds to a given CUDA-tile (not GIS-tile). 
         * (2)
         * The second kernel sum up the 3D change matrix returning one
         * 2D array being the accountancy for the whole ROI. 
         */

    	// Enable exceptions and omit all subsequent error checks
        JCudaDriver.setExceptionsEnabled(true);
                
        //System.out.println("ntilesX: "+ntilesX+"\tntilesY: "+ntilesY);
        int imap_bytes 	= imWidth * imHeight * Sizeof.BYTE;					//uint8_t
        int omap_bytes 	= imWidth * imHeight * Sizeof.INT;					//uint32_t
        int chmat_bytes	= numclasses * numclasses * imHeight * Sizeof.INT;	//uint32_t
        //System.out.println("mapsize: "+mapsize+"\tmapsizeb: "+mapsizeb);
        
        // Initialise the driver:
        cuInit(0);
        
        // Obtain the number of devices:
        int gpuDeviceCount[] = {0};
        cuDeviceGetCount( gpuDeviceCount );
        int deviceCount = gpuDeviceCount[0];
		if (deviceCount == 0) {
			System.out.println("error: no devices supporting CUDA.");
		    //exit();
		}
        //System.out.println("Found " + deviceCount + " devices");
        
        // Select first device, but I should select/split devices
        int selDev = 0;
        CUdevice device = new CUdevice();
        cuDeviceGet(device, selDev);
        
        // Get some useful properties: 
        int amountProperty[] = { 0 };
        // -1-
        cuDeviceGetAttribute(amountProperty, CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK, device);
        int maxThreadsPerBlock = amountProperty[0];
        //System.out.println("maxThreadsPerBlock: "+maxThreadsPerBlock);
        // -2-
        //cuDeviceGetAttribute(amountProperty, CU_DEVICE_ATTRIBUTE_TOTAL_CONSTANT_MEMORY, device);
        //int totalGlobalMem = amountProperty[0];
        //System.out.println("totalGlobalMem [to be corrected!!]: "+totalGlobalMem);
        
        // Create a context for the selected device
        CUcontext context = new CUcontext();
        //int cuCtxCreate_STATUS = 
        cuCtxCreate(context, selDev, device);
        //System.out.println("cuCtxCreate_STATUS: "+cuCtxCreate_STATUS);

        // Load the ptx file.
        //System.out.println("Loading ptx FILE...");
        CUmodule module = new CUmodule();
        //int cuModLoad = 
        cuModuleLoad(module, PTX_FILE_NAME);
        //System.out.println("cuModLoad: "+cuModLoad);
        
        // Obtain a function pointer to the "add" function.
        //System.out.println("changemap MOD");
        CUfunction changemap = new CUfunction();
        cuModuleGetFunction(changemap, module, "_Z9changemapPKhS0_S0_jjjjPjS1_");
        //System.out.println("changemat MOD");
        CUfunction changemat = new CUfunction();
        cuModuleGetFunction(changemat, module, "_Z9changematPjjj");

        // Allocate the device input data, and copy the
        // host input data to the device
        //System.out.println("dev_iMap1");
        CUdeviceptr dev_iMap1 = new CUdeviceptr();
        cuMemAlloc(dev_iMap1, imap_bytes );
        cuMemcpyHtoD(dev_iMap1, Pointer.to(host_iMap1), imap_bytes);
        //System.out.println("dev_iMap2");
        CUdeviceptr dev_iMap2 = new CUdeviceptr();
        cuMemAlloc(dev_iMap2, imap_bytes );
        cuMemcpyHtoD(dev_iMap2, Pointer.to(host_iMap2), imap_bytes);
        //System.out.println("dev_roiMap");
        CUdeviceptr dev_roiMap = new CUdeviceptr();
        cuMemAlloc(dev_roiMap, imap_bytes );
        cuMemcpyHtoD(dev_roiMap, Pointer.to(host_roiMap), imap_bytes);
        
        // Allocate device output memory
        //System.out.println("dev_oMap");
        CUdeviceptr dev_oMap = new CUdeviceptr();
        cuMemAlloc(dev_oMap, omap_bytes);
        //System.out.println("dev_chMat");
        CUdeviceptr dev_chMat = new CUdeviceptr();
        cuMemAlloc(dev_chMat, chmat_bytes);//ERROR with Integer.SIZE
      /*int host_chMat_3D[] = new int[numclasses * numclasses * ntilesX * ntilesY];
        int i;
        for(i=0;i<numclasses*numclasses* ntilesX * ntilesY;i++) {host_chMat_3D[i]=0;}
        cuMemcpyHtoD(dev_chMat, Pointer.to(host_chMat_3D), numclasses*numclasses* ntilesX * ntilesY*Sizeof.INT);        
      */cudaMemset(dev_chMat,0,chmat_bytes);
        
        // System.out.println("first kernel");
        // Set up the kernel parameters: A pointer to an array
        // of pointers which point to the actual values.
        Pointer kernelParameters1 = Pointer.to(
            Pointer.to(dev_iMap1),
            Pointer.to(dev_iMap2),
            Pointer.to(dev_roiMap),
            Pointer.to(new int[]{imWidth}),
            Pointer.to(new int[]{imHeight}),
            Pointer.to(new int[]{imWidth}),
            Pointer.to(new int[]{numclasses}),
            Pointer.to(dev_chMat),
            Pointer.to(dev_oMap)
        );

        //System.out.println("pointers done");
        // Call the kernel function.
        int blockSizeX 	= (int) Math.floor(Math.sqrt(maxThreadsPerBlock));									// 32
        int blockSizeY 	= (int) Math.floor(Math.sqrt(maxThreadsPerBlock));									// 32
        int blockSizeZ 	= 1;
        int gridSizeX 	= 1+ (int) Math.ceil( imHeight / (blockSizeX*blockSizeY) ); 
        int gridSizeY 	= 1;
        int gridSizeZ 	= 1;
        //System.out.println("launch cuda kernel");
        //int status_k1 = 
		cuLaunchKernel(changemap,
    		gridSizeX,  gridSizeY, gridSizeZ,   	// Grid dimension
    		blockSizeX, blockSizeY, blockSizeZ,     // Block dimension
            0, null,               					// Shared memory size and stream
            kernelParameters1, null 				// Kernel- and extra parameters
        );
        //System.out.println("	status k1 = "+status_k1);
        //System.out.println("	dev_chMat.len = "+dev_chMat.getByteBuffer(0, 4));
        //System.out.println("synchro");
        //int status_syn1 = 
		cuCtxSynchronize();
        //System.out.println("	synchro_1 = "+status_syn1);

        //System.out.println("second kernel");
        // Set up the kernel parameters: A pointer to an array
        // of pointers which point to the actual values.
        Pointer kernelParameters2 = Pointer.to(
            Pointer.to(dev_chMat),
            Pointer.to(new int[]{numclasses * numclasses}),
            Pointer.to(new int[]{imHeight})
            );
        //System.out.println("pointers done");
        //System.out.println("launch cuda kernel");
        //int status_k2 = 
		cuLaunchKernel(changemat,
    		gridSizeX,  gridSizeY, gridSizeZ,   	// Grid dimension
    		blockSizeX, blockSizeY, blockSizeZ,     // Block dimension
            0, null,               					// Shared memory size and stream
            kernelParameters2, null 				// Kernel- and extra parameters
        );
        //System.out.println("	status k2 = "+status_k2);
        //int status_syn2 = 
		cuCtxSynchronize();
        //System.out.println("	synchro_2 = "+status_syn2);

        // Allocate host output memory and copy the device output
        // to the host.
        int host_chMat[] = new int[numclasses * numclasses];
        //int cuMemcpy_oMap_STATUS = 
		cuMemcpyDtoH(Pointer.to(host_chMat), dev_chMat, chmat_bytes / imHeight);
        //System.out.println("cuMemcpy_oMap_STATUS: "+cuMemcpy_oMap_STATUS);
        int host_oMap[] = new int[imHeight * imWidth];
        //System.out.println("mapsize: "+mapsize);
        //int cuMemcpy_chMat_STATUS = 
        cuMemcpyDtoH(Pointer.to(host_oMap), dev_oMap, omap_bytes);
        //System.out.println("cuMemcpy_chMat_STATUS: "+cuMemcpy_chMat_STATUS);
        
        // Clean up.
        cuMemFree(dev_iMap1);
        cuMemFree(dev_iMap2);
        cuMemFree(dev_roiMap);
        cuMemFree(dev_oMap);
        cuMemFree(dev_chMat);
        
        // Unload MODULE
        //int cuModUnload_STATUS = 
        cuModuleUnload ( module );
        //System.out.println("cuModUnload_STATUS: "+cuModUnload_STATUS);
        
        // Destroy  CUDA context:
        //int cuDestroy_STATUS = 
        cuCtxDestroy( context );
        //System.out.println("cuDestroy_STATUS: "+cuDestroy_STATUS);
        
        // OUTPUT:
        return Arrays.asList(host_oMap,host_chMat);
    }
    
    
	/**
     * Creates an image from a vector of int
     * @param rect
     * @param data
     * @return
     */
    private RenderedImage createImage(Rectangle rect, final int[] data) {
        final SampleModel sm= new PixelInterleavedSampleModel(
                DataBuffer.TYPE_INT,
                rect.width,
                rect.height,
                1,
                rect.width,
                new int[]{0});
        final DataBufferInt db1= new DataBufferInt(data, rect.width*rect.height);
        final WritableRaster wr= 
            com.sun.media.jai.codecimpl.util.RasterFactory.createWritableRaster(sm, db1, new Point(0,0));
        final BufferedImage image= new BufferedImage(
        		ImageUtil.createColorModel(sm),
        		wr,
        		false,
        		null);

        return image;
    }
        
    
}