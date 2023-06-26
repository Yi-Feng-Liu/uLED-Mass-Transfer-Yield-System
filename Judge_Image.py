import pandas as pd
import numpy as np
import cv2
import matplotlib.pyplot as plt
from scipy.ndimage import label, center_of_mass


class Judge_image:
    """Input the LED type to recognize back ground color.
    
    Origin_arr will be used to plot border of defect cluster.
    
    label_arr will be used to count size of each cluter for exceed standard.
    
    standard is defect limit you want to filter.
    
    coc2_x & coc2_y & pixel_x & pixel_y are from specific product.json.
    """
    def __init__(self, led_type, origin_arr, standard, coc2_x, coc2_y, pixel_x, pixel_y):
        self.standard = standard
        self.coc2_x = coc2_x
        self.coc2_y = coc2_y
        self.pixel_x = pixel_x
        self.pixel_y = pixel_y
        self.led_type = led_type
        
        
        # original array
        self.arr = np.where(origin_arr==0, 1 ,0)
        self.arr = np.asarray(self.arr, dtype='uint8')
        self.kernel = np.ones((3,3), dtype=bool)
        # label array
        self.label_arr, self.numfeature = label(input=self.arr, structure=self.kernel)


              
    def leave_exceed_defect(self):
        """Remove the defect & label index that lower standard.
        
        Only leave the defect index and count of exceed standard and return a dict.

        Args:
            standard (int): set the limit of the defect.
            labeled_array (npt.ArrayLike): the array have been label.

        Returns:
            filter_dict (dict): filtered cluster index and count
        """
        
        # leave the unique label and return the label count which belong label
        unique, counts = np.unique(self.arr, return_counts=True)
        label_counts = dict(zip(unique, counts))
        
        # filterd clutser index and defect count
        filter_dict = dict(filter(lambda elem: (elem[1] >= self.standard) & (elem[0] != 0), label_counts.items()))
        return filter_dict
        

    def get_each_center_of_mass(self, iter:int):
        """Get each cluster center point position and return x & y coordination. 

        Args:
            original_arr (npt.ArrayLike): original 2D array
            labeled_arr (npt.ArrayLike): labeled 2D array
            iter (int): iterable 

        Returns:
            coordination_x & coordination_y: int, int
        """
        
        center = center_of_mass(input=self.arr, labels=self.label_arr, index=iter)
        center_y, center_x = int(center[0]), int(center[1])
        return center_x, center_y
    
    
    def calculate_each_border_size(self, iter:int):
        """Calculate the labeled defect border from each coordination.

        Args:
            labeled_arr (npt.ArrayLike): labeled 2D array
            iter (int): iterable

        Returns:
            rect_height, rect_width: int & int
        """
        
        y, x = np.where(self.label_arr == iter)
        rect_height = (np.max(y) - np.min(y)) + 10
        rect_width = (np.max(x) - np.min(x)) + 10
        return rect_height, rect_width


    def get_center_list(self):
        filter_dict = self.leave_exceed_defect()
        centers_of_cluster = center_of_mass(self.arr, self.label_arr, range(1, self.numfeatures+1))
        # print coordination for each center of cluster
        center_ls = []
        for i, center in enumerate(centers_of_cluster):
            if i+1 in list(filter_dict.keys()):
                print(f'Cluster index {i+1}: \nCenter coordinate ({int(center[0])}, {int(center[1])})')
                center_ls.append(center)
        return center_ls
    
    
    def get_area_cluster_count(self):
        center_list = self.get_center_list()
        coord = {}
        cnt_area_defect_cluster = {}
        
        # area name ex: A=65, B=66, C=67, D=68
        self.area_name = 65 
        for j in range(self.coc2_y):
            for i in range(self.coc2_x):
                # initialize coordinate
                coord[chr(self.area_name)] = [(self.pixel_y*j, self.pixel_x*i), (self.pixel_y*(j+1), self.pixel_x*(i+1))]
                # initialize count of defect cluster
                cnt_area_defect_cluster[chr(self.area_name)] = 0 
                self.area_name += 1
        for center in center_list:
            for key, value in coord.items():
                if coord[key][0][0] < center[0] < coord[key][1][0] and coord[key][1][0] < center[1] < coord[key][1][1]:
                    cnt_area_defect_cluster[key] += 1
        return cnt_area_defect_cluster
                            
        
    def plot_defect_border(self, numfeature:int):
        """Plot the each cluster border and show the defect count.

        Args:
            numfeature (int): Total index of the defect map have been labeled.
        """
        fig, ax = plt.subplots()
        for i in range(numfeature):
            size = np.sum(self.label_arr == i+1)
            if size < self.standard:
                continue
            
            center_x, center_y = self.get_each_center_of_mass(iter=i+1)
            rect_h, rect_w = self.calculate_each_border_size(iter=i+1)
            
            ax.add_patch(
                plt.Rectangle(
                    (center_x - rect_w/2, center_y - rect_h/2), rect_w, rect_h, fill=False, edgecolor='lime', lw=2,
                )
            )
            ax.text(
                (center_x-rect_w/2)+3, (center_y-rect_h/2)-3, size, color='black', fontsize=8, bbox=dict(facecolor='lime', pad=2, edgecolor='none')
            )
        if self.led_type == 'R':    
            cmap = plt.cm.colors.ListedColormap(['red', 'black']) 
        elif self.led_type == 'G':
            cmap = plt.cm.colors.ListedColormap(['green', 'black']) 
        else:
            cmap = plt.cm.colors.ListedColormap(['blue', 'black']) 
            
        plt.imshow(self.arr, cmap=cmap)    
        plt.savefig("test3.png", bbox_inches='tight')
        plt.show()
