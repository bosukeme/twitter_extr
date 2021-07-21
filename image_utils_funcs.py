import numpy as np
import urllib
import cv2
import os
import random
from PIL import Image, ImageFont, ImageDraw, ImageFilter
from sys import platform

"""
Add timeout functionaltiy
"""
from functools import wraps
import errno
import os
import signal

class TimeoutError(Exception):
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):

            try:
                signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(seconds)
            except:
                pass

            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator

"""
Notes
- resize_and_crop_article_image has been removed and as such 
"""
def generate_blank_image(height, width, colour):
    """
    This function takes image dimensions and a user-defined colour and then returns
    a blank image of the input colour
    """
    blank_image = np.zeros((height,width,3), np.uint8)
    blank_image[:] = colour
    return blank_image

#@timeout(5, os.strerror(errno.ETIMEDOUT))
def url_to_image(url, temp_article_image_path):
    """
    This has been updated compared to the previous one to be able to 
    extract more difficult image links via initialising a browser object.
    """
    opener = urllib.request.URLopener()
    opener.addheader('User-Agent', 'whatever')
    
    try:
        filename, headers = opener.retrieve(url,temp_article_image_path)
    except:
        urllib.request.urlretrieve(url,temp_article_image_path)

    image = cv2.imread(temp_article_image_path)
    image = cv2.cvtColor(np.uint8(image), cv2.COLOR_BGR2RGB)

    return image


def calculate_image_sharpness(image):
    """
    This function takes in an image as a numpy array and then calculates
    a sharpness score (likely based on the density of the pixels?)
    """
    laplacian = cv2.Laplacian(image, cv2.CV_64F)
    gnorm = np.sqrt(laplacian**2)
    sharpness = np.average(gnorm)
    
    return sharpness


def crop_image_by_centre(image, target_height, target_width):
    """
    This function takes a target height and width and then crops an image
    to fit the scales dimensions of the target height and width.
    """
    original_height = image.shape[0] 
    original_width = image.shape[1]

    ratio = target_height/target_width 

    new_height = original_height
    new_width = int(new_height/ratio)

    original_width_centre = int(original_width/2)

    start_width_loc = original_width_centre - int(new_width/2)
    end_width_loc = original_width_centre + int(new_width/2)

    cropped_img = image[:,start_width_loc:end_width_loc]

    return cropped_img


def crop_max_square_and_resize_image(image_url, temp_image_path, target_square_size):
    """
    This function takes an image, crops the maximum square and then resizes it to the target image size
    """
    squared_image = url_to_image(image_url, temp_image_path)

    # Get the central square of the image
    squared_image = Image.fromarray(squared_image)
    squared_image = crop_max_square(squared_image).resize((target_square_size, target_square_size), Image.LANCZOS)
    squared_image = squared_image.convert("RGB")
    squared_image = cv2.cvtColor(np.uint8(squared_image), cv2.COLOR_BGR2RGB)
    squared_image = cv2.cvtColor(np.uint8(squared_image), cv2.COLOR_BGR2RGB)
    
    return squared_image

def resize_and_crop_image(target_height, target_width, image):
    """
    This function resizes the image and then crops it focusing around the centre
    - This should go to image utils
    """
    image = crop_image_by_centre(image, target_height, target_width)
    
    image = cv2.resize(image, (target_width,target_height))
    
    return image


def create_colour_fade_image(original_img, orig_mask, colour, colour_mask):
    """
    This takes any image and creates a lighter tone to the image
    """

    white_img = np.zeros(shape = (original_img.shape[0],original_img.shape[1],3), dtype=np.uint8)
    white_img[:] = colour

    masked_image = cv2.addWeighted(original_img, orig_mask, white_img, colour_mask, 0)
    
    return masked_image.copy() 


def offset_vstack_two_images(image1, image2, offset):
    """
    This function takes two images and vertically stacks them based on a subset of each
    image which is determined by the offset
    """
    img1_new = image1[0:offset,:]
    img2_new = image2[offset:,:]
    temp_img = np.vstack((img1_new,img2_new))
    
    return temp_img


def offset_hstack_two_images(image1, image2, offset):
    """
    This function takes two images and horizontally stacks them based on a subset of each
    image which is determined by the offset
    """
    img1_new = image1[:,0:offset]
    img2_new = image2[:,offset:]
    temp_img = np.hstack((img1_new,img2_new))
    
    return temp_img


def create_image_blend(img1, img1_mask, img2, img2_mask):
    """
    This function takes two images and creates a blend between them based on the values
    of the mask for image1 and image2
    """

    masked_image = cv2.addWeighted(img1, img1_mask, img2, img2_mask, 0)
    
    return masked_image.copy() 


def paste_transparent_image_to_background(original_image, paste_image, image_x_start, image_y_start, size = (0,0)):
    """
    This reads in a transparent PIL image and pastes it into an input image
    based on the start coordinates provided. The option to resize the transparent
    image is also available if resize parameters are provided.
    
    ** Its important that the paste image is a transparent file thats either a read in png
    or we have already run functionality to make it transparent. We should think about a test that can be done to check that an image is transparent
    """
    # Convert original image to PIL format
    original_image = Image.fromarray(original_image)
    
    # Paste transparent image into the original image
    original_image.paste(paste_image, (image_x_start, image_y_start), paste_image) 
    
    original_image = original_image.convert("RGB")
    original_image = cv2.cvtColor(np.uint8(original_image), cv2.COLOR_BGR2RGB)
    original_image = cv2.cvtColor(np.uint8(original_image), cv2.COLOR_BGR2RGB)

    return original_image


def crop_image_from_centre_to_target_ratio(image, target_height, target_width):
    """
    This function to take any image and then crop out an image from the centre. The function
    takes any target height and width, and crops out a proportional image (from the centre) that
    will then be resized to the target height and width. This technically works for any target dimensions.
    In our case, this function would work perfectly well for vertical as well as square formats
    """
    
    image_height = image.shape[0]
    image_width = image.shape[1]
    expected_height = image_height
    height_ratio = target_height/image_height
    expected_width = int(target_width/height_ratio)
    target_ratio = target_height/target_width
    
    if image_height <= image_width:
        """
        We may need to do the same conditions on the target ratio as below but it seems
        to all work fine for now
        """
        expected_height = image_height
        height_ratio = target_height/image_height
        expected_width = int(target_width/height_ratio)

        # Find the mid point of the image
        image_width = image.shape[1]
        mid_point = int(image_width/2)

        # Now crop the image around the mid point
        crop_x_start = mid_point - int(expected_width/2)
        crop_x_end = mid_point + int(expected_width/2)
        cropped_img = image[:, crop_x_start:crop_x_end]
        
    elif image_height > image_width:
        
        if target_ratio <= 1: # In this case the width is equal to or greater than the height
            expected_width = image_width
            width_ratio = target_width/image_width
            expected_height = int(target_height/width_ratio)

            # Find the mid point of the image
            image_height = image.shape[0]
            mid_point = int(image_height/2)

            # Now crop the image around the mid point
            crop_y_start = mid_point - int(expected_height/2)
            crop_y_end = mid_point + int(expected_height/2)
            cropped_img = image[crop_y_start:crop_y_end, :]
            
        else: # In this case, the target height is greater than the target width
            expected_height = image_height
            height_ratio = target_height/image_height
            expected_width = int(target_width/height_ratio)

            # Find the mid point of the image
            image_width = image.shape[1]
            mid_point = int(image_width/2)

            # Now crop the image around the mid point
            crop_x_start = mid_point - int(expected_width/2)
            crop_x_end = mid_point + int(expected_width/2)
            cropped_img = image[:, crop_x_start:crop_x_end]
            

    # Resize the cropped image to target dimensions
    cropped_img = cv2.resize(cropped_img.copy(), (target_width,target_height))

    return cropped_img


def change_image_non_white_colour(image_path, colour):
    """
    This function takes any image with a white background and can change all the non-white colours in the
    image to your desired colour. Its important to note that this function takes a file path.
    """
    
    image = cv2.imread(image_path)
    image = cv2.cvtColor(np.uint8(image), cv2.COLOR_BGR2RGB)

    temp1 = image.copy()
    temp2 = image.copy()

    white_pixels_mask = np.all(temp1 == [255, 255, 255], axis=-1)

    non_white_pixels_mask = np.any(temp1 != [255, 255, 255], axis=-1)  

    temp2[white_pixels_mask] = [255, 255, 255]
    temp2[non_white_pixels_mask] = colour

    return temp2.copy()


def crop_center(img, crop_width, crop_height):
    """
    This function takes an image and crops a square around it based on the provided dimensions
    """
    img_width, img_height = img.size
    return img.crop(((img_width - crop_width) // 2,
                         (img_height - crop_height) // 2,
                         (img_width + crop_width) // 2,
                         (img_height + crop_height) // 2))


def crop_max_square(img):
    """
    This function gets the largest possible square (centred around the centre) in the image
    """
    return crop_center(img, min(img.size), min(img.size))


def expand2square(pil_img, background_color):
    """
    This function takes any image and adds padding based on the colour provided (not used for now)
    """
    width, height = pil_img.size
    if width == height:
        return pil_img
    elif width > height:
        result = Image.new(pil_img.mode, (width, width), background_color)
        result.paste(pil_img, (0, (width - height) // 2))
        return result
    else:
        result = Image.new(pil_img.mode, (height, height), background_color)
        result.paste(pil_img, ((height - width) // 2, 0))
        return result
    
    
def mask_circle_solid(pil_img, background_color, blur_radius, offset=0):
    """
    Use composite() to composite two images according to the mask image.
    Related post: Composite two images according to a mask image with Python, Pillow - https://note.nkmk.me/en/python-pillow-composite/
    
    Draw a circle and use it as a mask image. For details on drawing, see the following post.
    Related post: Draw circle, rectangle, line etc with Python, Pillow - https://note.nkmk.me/en/python-pillow-imagedraw/
    
    Create a single color plain image of the desired background color with new() and composite it with the square image with a circular mask.

    The border is smoothed by blurring the mask image with ImageFilter. Since the area of the circle spreads when it blurs, it is necessary to draw a smaller circle first.

    Define the following function. Specify the background color background_color, the size of the blur blur_radius, and the offset offset. No blur with blur_radius=0.
    """
    background = Image.new(pil_img.mode, pil_img.size, background_color)

    offset = blur_radius * 2 + offset
    mask = Image.new("L", pil_img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((offset, offset, pil_img.size[0] - offset, pil_img.size[1] - offset), fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(blur_radius))

    return Image.composite(pil_img, background, mask)


def mask_circle_transparent(pil_img, blur_radius, offset=0):
    offset = blur_radius * 2 + offset
    mask = Image.new("L", pil_img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((offset, offset, pil_img.size[0] - offset, pil_img.size[1] - offset), fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(blur_radius))

    result = pil_img.copy()
    result.putalpha(mask)

    return result


def add_corners(image, rad):
    """
    This function takes an image and then the radius of the corner and crops out corners around the edges of the image.
    
    Add extra functionality later that allows you to decide if you want to smoothen the corners or not, it works fine for normal
    ages, but when you are using a black background mask it may create some issues. For now we will comment out the smoothen functionality
    but we should come back and review this later.
    """
    circle = Image.new('L', (rad * 2, rad * 2), 0)
    draw = ImageDraw.Draw(circle)
    draw.ellipse((0, 0, rad * 2, rad * 2), fill=255)
    alpha = Image.new('L', image.size, 255)
    w, h = image.size
    alpha.paste(circle.crop((0, 0, rad, rad)), (0, 0))
    alpha.paste(circle.crop((0, rad, rad, rad * 2)), (0, h - rad))
    alpha.paste(circle.crop((rad, 0, rad * 2, rad)), (w - rad, 0))
    alpha.paste(circle.crop((rad, rad, rad * 2, rad * 2)), (w - rad, h - rad))
    image.putalpha(alpha)
#     image = image.filter(ImageFilter.GaussianBlur(radius=0.75)) # just added
    return image


def generate_transparent_circlular_crop_from_image(image_path, target_len, background_color, blur_radius, circle_offset):
    """
    This function takes an image and crops out the maximal circle (taking the offset into account), resizes the image to 
    the target len and then makes the image transparent. Its important to note that this returns a transparent image. The blur 
    radius determines how much of the outer parts of the circular image are to be blurred
    """
    
    # Read in the user profile image
    image = Image.open(image_path)

    # Get the largest available square from the centre and then resize to the 'target_len'
    image = crop_max_square(image).resize((target_len, target_len), Image.LANCZOS)

    # Create a circle border around the user image and set the background colour 
    circle_solid_image = mask_circle_solid(image, background_color, blur_radius, offset=circle_offset) # Offset is created so that the boundary image can fit in well

    # Make the background to be transparent
    circle_image = mask_circle_transparent(circle_solid_image, blur_radius, offset=0)

    return circle_image



"""
Functions for dealing with transparent images
"""

def has_transparency(img):
    """
    This function takes in an input image and checks if its transparent
    """
    if img.mode == "P":
        transparent = img.info.get("transparency", -1)
        for _, index in img.getcolors():
            if index == transparent:
                return True
    elif img.mode == "RGBA":
        extrema = img.getextrema()
        if extrema[3][0] < 255:
            return True

    return False


def get_dominant_colour(a):
    """
    This function returns the most dominant colour in an image as [R,G,B]
    """
    colors, count = np.unique(a.reshape(-1,a.shape[-1]), axis=0, return_counts=True)
    return colors[count.argmax()]


def change_primary_colour_to_white(img, dom_colour_rgb):
    """
    This function takes in an image and changes all instances of the primary colour to white. 
    Should go to image utils
    """
    # Define lower and uppper limits of what we call "brown"
    lower = (int(dom_colour_rgb[0]-10), int(dom_colour_rgb[1]-10), int(dom_colour_rgb[2]-10))
    upper = (int(dom_colour_rgb[0]+10), int(dom_colour_rgb[1]+10), int(dom_colour_rgb[2]+10))

#     # Define lower and uppper limits of what we call "brown"
#     lower = (int(dom_colour_rgb[0]-40), int(dom_colour_rgb[1]-40), int(dom_colour_rgb[2]-40))
#     upper = (int(dom_colour_rgb[0]+40), int(dom_colour_rgb[1]+40), int(dom_colour_rgb[2]+40))
    
    
    # create the mask and use it to change the colors
    mask = cv2.inRange(img.copy(), lower, upper)
    img[mask != 0] = [255,255,255]
    
    return img


def colour_to_transparency(img, transparency_flag, transparency_level=255):
    """
    Takes an image and converts all the white pixels in the frame to transparent
    """
    x = np.asarray(img.convert('RGBA')).copy()

    x[:, :, 3] = (transparency_level * (x[:, :, :3] != transparency_flag).any(axis=2)).astype(np.uint8)

    return Image.fromarray(x)


def white_to_transparency(img):
    """
    Takes an image and converts all the white pixels in the frame to transparent
    """
    x = np.asarray(img.convert('RGBA')).copy()

    x[:, :, 3] = (255 * (x[:, :, :3] != 255).any(axis=2)).astype(np.uint8)

    return Image.fromarray(x)


def white_to_transparency_gradient(img):
    """
    Takes an image and converts all the white pixels in the frame to transparent. However
    this time its done with a gradient so the paste image appears translucent
    """
    x = np.asarray(img.convert('RGBA')).copy()

    x[:, :, 3] = (255 - x[:, :, :3].mode(axis=2)).astype(np.uint8)

    return Image.fromarray(x)


def make_image_transparent(gif_image, white = False, dom_colour_rgb='NA'):
    """
    Takes in an image and makes it transparent based on the dominant colour in the image.
    This works best for images that have a single background colour. If the image is already
    white, then you just go ahead and make it transparent
    """
    if white == False:
        gif_image = change_primary_colour_to_white(gif_image, dom_colour_rgb)
    
    # Test the image segmentation to see if it works
    gif_image = Image.fromarray(gif_image)
    gif_image = gif_image.convert("RGBA")
    
    gif_image = white_to_transparency(gif_image)
    
    return gif_image
    

def paste_transparent_gif_image(input_img, gif_image, gif_x_start, gif_y_start, dom_colour_rgb, video_width, resize_mult):
    """
    This takes an image of a gif on a solid background, then crops it out and pastes to another background. We would also resize it based
    on the resize multiple
    """
    output_img = img_utils.paste_transparent_image_to_background(input_img, paste_image, x_start, y_start, size = (0,0))
    
    return output_img


def determine_brand_colours(brand_logo_url):
    """
    This function takes in a url for a brand logo image, and then returns the primarly and secondary
    in hexadecimal based on the colours in the logo image
    """
    # Read in the logo image
    temp_article_image_path = '%s/temp_article_image.jpg' % os.getcwd()
#     print(temp_article_image_path)
    logo_image = url_to_image(brand_logo_url, temp_article_image_path)
    
    # Get frequency of all colours found in the image
    colours, count = np.unique(logo_image.reshape(-1,logo_image.shape[-1]), axis=0, return_counts=True)
    
    # Sort the colours and get the most common non-white colour as the primary colour
    sorted_indices = sorted(range(len(count)), key=lambda k: count[k], reverse=True)
    sorted_indices2 = sorted_indices[0:10]
    sorted_count = [count[i] for i in sorted_indices2]
    sorted_colours = [tuple(colours[i]) for i in sorted_indices2]
    sorted_colours = [colour for colour in sorted_colours if colour != (255, 255, 255)]
    primary_colour = sorted_colours[0]
    
    ## Determine the secondary colour based on how close the primary colour is to being white
    ## For now the secondary colour is either black or white
    white = (255,255,255)
    black = (0,0,0)
    white_diff = np.subtract(white, primary_colour)
    if sum(white_diff) > 150:
        secondary_colour = white
    else:
        secondary_colour = black
        
#     print(primary_colour)
    primary_colour_hex = '#%02x%02x%02x' % primary_colour
#     print(secondary_colour)
    secondary_colour_hex = '#%02x%02x%02x' % secondary_colour
    
    return primary_colour_hex, secondary_colour_hex

"""
For Later
This would actually be quite good when you have the Bloverse stories in play
and users are creating stories without an image, we would automatically try and
use the complementary colour and see how that comes out looking. Should be fire!.
However if they choose white or black then we may need to figure that bitch out... maybe by default it would have the Bloverse colours
"""
from colorsys import rgb_to_hsv, hsv_to_rgb

def complementary(r, g, b):
   #returns RGB components of complementary color
   hsv = rgb_to_hsv(r, g, b)
   return hsv_to_rgb((hsv[0] + 0.5) % 1, hsv[1], hsv[2])

"""
All above is as of the last push on the engine with voiceovers etc etc
"""
def draw_rounded_rectangle(src, top_left, bottom_right, radius=1, color=255, thickness=1, line_type=cv2.LINE_AA):
    """
    This function takes a start location and end location 
    """
    #  corners:
    #  p1 - p2
    #  |     |
    #  p4 - p3

    p1 = top_left
    p2 = (bottom_right[1], top_left[1])
    p3 = (bottom_right[1], bottom_right[0])
    p4 = (top_left[0], bottom_right[0])

    height = abs(bottom_right[0] - top_left[1])

    if radius > 1:
        radius = 1

    corner_radius = int(radius * (height/2))

    if thickness < 0:

        #big rect
        top_left_main_rect = (int(p1[0] + corner_radius), int(p1[1]))
        bottom_right_main_rect = (int(p3[0] - corner_radius), int(p3[1]))

        top_left_rect_left = (p1[0], p1[1] + corner_radius)
        bottom_right_rect_left = (p4[0] + corner_radius, p4[1] - corner_radius)

        top_left_rect_right = (p2[0] - corner_radius, p2[1] + corner_radius)
        bottom_right_rect_right = (p3[0], p3[1] - corner_radius)

        all_rects = [
        [top_left_main_rect, bottom_right_main_rect], 
        [top_left_rect_left, bottom_right_rect_left], 
        [top_left_rect_right, bottom_right_rect_right]]

        [cv2.rectangle(src, rect[0], rect[1], color, thickness) for rect in all_rects]

    # draw straight lines
    cv2.line(src, (p1[0] + corner_radius, p1[1]), (p2[0] - corner_radius, p2[1]), color, abs(thickness), line_type)
    cv2.line(src, (p2[0], p2[1] + corner_radius), (p3[0], p3[1] - corner_radius), color, abs(thickness), line_type)
    cv2.line(src, (p3[0] - corner_radius, p4[1]), (p4[0] + corner_radius, p3[1]), color, abs(thickness), line_type)
    cv2.line(src, (p4[0], p4[1] - corner_radius), (p1[0], p1[1] + corner_radius), color, abs(thickness), line_type)

    # draw arcs
    cv2.ellipse(src, (p1[0] + corner_radius, p1[1] + corner_radius), (corner_radius, corner_radius), 180.0, 0, 90, color ,thickness, line_type)
    cv2.ellipse(src, (p2[0] - corner_radius, p2[1] + corner_radius), (corner_radius, corner_radius), 270.0, 0, 90, color , thickness, line_type)
    cv2.ellipse(src, (p3[0] - corner_radius, p3[1] - corner_radius), (corner_radius, corner_radius), 0.0, 0, 90,   color , thickness, line_type)
    cv2.ellipse(src, (p4[0] + corner_radius, p4[1] - corner_radius), (corner_radius, corner_radius), 90.0, 0, 90,  color , thickness, line_type)

    return src


def add_sp_noise_to_image(image,prob):
    '''
    Add salt and pepper noise to image
    prob: Probability of the noise
    '''
    output = np.zeros(image.shape,np.uint8)
    thres = 1 - prob 
    for i in range(image.shape[0]):
        for j in range(image.shape[1]):
            rdn = random.random()
            if rdn < prob:
                output[i][j] = 0
            elif rdn > thres:
                output[i][j] = 255
            else:
                output[i][j] = image[i][j]
            
    # Now convert the output image into an acceptable format
    output = Image.fromarray(output)
    output = output.convert("RGB")
    output = cv2.cvtColor(np.uint8(output), cv2.COLOR_BGR2RGB)
    output = cv2.cvtColor(np.uint8(output), cv2.COLOR_BGR2RGB)
    return output


def get_complementary_color(my_hex):
    """Returns complementary RGB color

    Example:
    >>>complementaryColor('FFFFFF')
    '000000'
    """
    if my_hex[0] == '#':
        my_hex = my_hex[1:]
    rgb = (my_hex[0:2], my_hex[2:4], my_hex[4:6])
    comp = ['%02X' % (255 - int(a, 16)) for a in rgb]
    return ''.join(comp)


def paste_transparent_image_to_background(original_image, paste_image, image_x_start, image_y_start):
    """
    This reads in a transparent PIL image and pastes it into an input image
    based on the start coordinates provided. The option to resize the transparent
    image is also available if resize parameters are provided.
    
    ** Its important that the paste image is a transparent file thats either a read in png
    or we have already run functionality to make it transparent. We should think about a test that can be done to check that an image is transparent
    """
    # Convert original image to PIL format
    original_image = Image.fromarray(original_image)
    
    # Paste transparent image into the original image
    original_image.paste(paste_image, (image_x_start, image_y_start), paste_image) 
    
    original_image = original_image.convert("RGB")
    original_image = cv2.cvtColor(np.uint8(original_image), cv2.COLOR_BGR2RGB)
    original_image = cv2.cvtColor(np.uint8(original_image), cv2.COLOR_BGR2RGB)

    return original_image


def add_rotated_text_to_image(subclip_image, watermark_font_size, watermark_font_path, watermark_text):
    """
    This function can be tweaked to add rotated text to an image
    """
    # Get the watermark height and width offsets
    img_width = subclip_image.shape[1]
    watermark_height_offset = 20
    watermark_text_offset = img_width - 5

    # convert to pillow image
    pillowImage = Image.fromarray(subclip_image.copy())

    # draw the text
    font = ImageFont.truetype(watermark_font_path, watermark_font_size)
    text_start_y = watermark_height_offset
    text_start_x = img_width - 5
    draw_rotated_text(pillowImage, 270, (text_start_x, text_start_y), watermark_text, (255, 255, 255), font=font)
    
    return pillowImage