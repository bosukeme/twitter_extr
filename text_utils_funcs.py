from PIL import Image, ImageFont, ImageDraw
import cv2
import numpy as np
import sys
## Build functionality to find the similarity between any two strings
from difflib import SequenceMatcher

#sys.path.insert(0, '/Users/dr_d3mz/Documents/GitHub/Bloverse Video Engine/General Functions') # insert the path to your functions folder

# Import modules
import aws_s3_funcs as aws_s3

"""
To Do: An improvement to the function that writes text onto images, we currently have 20 functions based on how many lines there are in the text.
       We should be able to create a function that dynamically does this based on len(line_list). It would iteratively loop through each line, and then
       there would also be a counter as well that allows you to calculate the 
"""

def split_text(text, text_font_dict):
    """
    This function takes a font and a width limit and then split any text provided into a technically unlimited number of lines
    """
    
    line_break_ind = []
    line_list = []
    
    words = text.split()
    font_path = text_font_dict['Font Path']
    font_size = text_font_dict['Font Size']
    width_limit = text_font_dict['Width Limit']
    
    font = ImageFont.truetype(font_path,font_size)
    
    for i in range(len(words)):

        if len(line_break_ind) < 1:
            next_text_string = ' '.join(words[:i+2])
            line1_w,line1_h = font.getsize(next_text_string)

            if line1_w > width_limit:
                line_string = ' '.join(words[:i+1])
                line_list.append(line_string)
                line_break_ind.append(i)
        else:
            text_string = ' '.join(words[line_break_ind[-1]+1:i+2])
            line1_w,line1_h = font.getsize(text_string)

            if line1_w > width_limit:
                line_string = ' '.join(words[line_break_ind[-1]+1:i+1])
                line_list.append(line_string)
                line_break_ind.append(i)

    # Add the final line of the text to the list
    if len(line_break_ind) > 0:
        line_list.append(' '.join(words[line_break_ind[-1]+1:]))
    else:
        line_list.append(text)
    
    return line_list


def shorten_text(text, threshold):
    """
    This takes any text and shortens it based on the threshold provided
    """
    text_len = len(text)

    if text_len > threshold:

        words = text.split()
        count = 0
        for i in range(len(words)):
            count += (len(words[i]))
            count += 1

            if count+1 > threshold:
                text_lim = i-1
                break
                
        shortened_text = ' '.join(words[0:text_lim+1])
        text = '%s ...' % shortened_text
        return text
    
    else:
        return text
    
def get_string_similarity_score(string_a, string_b):
    """
    This function takes two strings and returns their similarity score
    """
    return SequenceMatcher(None, string_a, string_b).ratio()

def calculate_target_font_ratios(bucket_name, folder_name, target_font_name, article_folder_path):
    """
    This takes target font name as the brand, creator or article font, and then calculates the width ratio
    based on the difference in font size between the baseline font of OpenSans-ExtraBold and the target font.
    We have also improved this function so that it picks the fonts from s3, instead of a local folder.
    ** This should be placed in the general functions folder, probably under text_funcs, or you can create
       a new one called font_funcs
    """
    # Read in the baseline font
    example_text = 'This is a sample text'
    baseline_font_name = 'OpenSans-ExtraBold.ttf'
    baseline_font_path = '%s/%s' % (article_folder_path, baseline_font_name)
    try:
        text_font = ImageFont.truetype(baseline_font_path, 10)
    except:
        aws_s3.download_s3_file(bucket_name, folder_name, baseline_font_name, baseline_font_path)
    baseline_font_dict1 = {
        'Font Size':48,
        'Width Limit':700,
        'Font Path':'%s' % baseline_font_path
    }
    baseline_font_path = baseline_font_dict1['Font Path']
    baseline_font_size = baseline_font_dict1['Font Size']
    baseline_text_font = ImageFont.truetype(baseline_font_path, baseline_font_size)

    ## Get the target font
    target_font_path = '%s/%s' % (article_folder_path, target_font_name)
    try:
        # Checks if we already have the target font downloaded
        text_font = ImageFont.truetype(target_font_path, 10)
    except:
        # If not then we download it from s3
        aws_s3.download_s3_file(bucket_name, folder_name, target_font_name, target_font_path)

    target_font_dict = {
        'Font Size':48,
        'Width Limit':700,
        'Font Path':'%s' % target_font_path
    }

    target_font_path = target_font_dict['Font Path']
    target_font_size = target_font_dict['Font Size']
    target_text_font = ImageFont.truetype(target_font_path, target_font_size)

    # Test by writing the font on something to see how its coming up... but best test is to just run it on the ting sha
    base_txt_line_w,base_txt_line_h = baseline_text_font.getsize(example_text)

    # Now see what the parameters are for the new font
    target_txt_line_w,target_txt_line_h = target_text_font.getsize(example_text)

    # Now calculate the height and width ratios
    height_ratio = round(base_txt_line_h/target_txt_line_h, 2)
    width_ratio = round(base_txt_line_w/target_txt_line_w, 2)

    return height_ratio, width_ratio


"""
These functions are related to generating text animations
"""
def get_line_text_image(line_ind, image, line_list, max_height, height_offset, text_font, text_colour, text_adjustment, y_anim_offset= 0, y_offset = 0, width_mult=0.1, colour_alpha=255):
    """
    This takes:
        - line_list: a list of text that we want to paste on an image
        - line_ind: the subset of the line_list that we want to paste on the image
        - colour_alpha: the level of opaqueness that we want in the image (255 means the image is fully opaque)
        - colour: the colour of the text
        - y_offset: for certain animations, we would aim to offset the height from the expected final height
        - text_adjustment: the orientation of the text, for now it can either be left or center adjuster
        - height_offset: the offset from the image where we want the first line of the text to appear
        - y_anim_offset: for text animations that gradually fade in whilst rising, this depicts the offset from the expected line height
        - max_height: the maximum height of each line, this also factors in spacing to ensure that the text doesnt overlap
    
    And then pastes all the lines of text on the input image
    """
    # Initialise the text colour based on the input colour and the colour alpha
    text_colour_w_alpha = (text_colour[0], text_colour[1], text_colour[2], colour_alpha)
    temp_line_list = line_list[0:line_ind]


    # Calculate the line width boundaries
    width_boundary_list = []
    if text_adjustment == 'Center':
        for line in temp_line_list:
            line_w,temp_h = text_font.getsize(line)
            txt_offset = line_w+len(line)
            width_boundary = int((image.shape[1] - txt_offset)/2)
            width_boundary_list.append(width_boundary)
    elif text_adjustment == 'Left':
        for line in temp_line_list:
            width_boundary = int(image.shape[1]*width_mult)
            width_boundary_list.append(width_boundary)

    # Calculate the line height boundaries
    line_height_list = []
    for i in range(len(temp_line_list)):
        ## Calculate the height offsets
        line_h = int(height_offset + (max_height * i) + y_anim_offset)
        line_height_list.append(line_h)

    # Now add the text to the image
    temp = image.copy()
    alpha = Image.new('RGBA', (temp.shape[1], temp.shape[0]), (255,255,255,0))
    temp = Image.fromarray(temp).convert("RGBA")
    draw = ImageDraw.Draw(alpha)

    for i in range(len(temp_line_list)):
        width = width_boundary_list[i]
        height = line_height_list[i]
        curr_line = temp_line_list[i]
        draw.text(((width,int(height-y_offset))), curr_line, fill=text_colour_w_alpha, font=text_font) 

    new_image = Image.alpha_composite(temp, alpha) 
    new_image = new_image.convert("RGB")
    new_image = cv2.cvtColor(np.uint8(new_image), cv2.COLOR_BGR2RGB)
    new_image = cv2.cvtColor(np.uint8(new_image), cv2.COLOR_BGR2RGB)
    
    return new_image


def generate_one_liner(image, line, line_h, width_boundary, text_font, text_colour, colour_alpha=255):
    """
    This takes a single line of a text and pastes onto the input image.
    *** Move this to the anim_utils when you are doing cleanups after concluding work on template 1.
    """
    # Decipher the colour
    text_colour_w_alpha = (text_colour[0], text_colour[1], text_colour[2], colour_alpha)
    
    # Now add the text to the image
    temp = image.copy()
    alpha = Image.new('RGBA', (temp.shape[1], temp.shape[0]), (255,255,255,0))
    temp = Image.fromarray(temp).convert("RGBA")
    draw = ImageDraw.Draw(alpha)
    
    # Paste the line text on the image
    draw.text(((width_boundary,line_h)), line, fill=text_colour_w_alpha, font=text_font) 
    
    new_image = Image.alpha_composite(temp, alpha) 
    new_image = new_image.convert("RGB")
    text_block = cv2.cvtColor(np.uint8(new_image), cv2.COLOR_RGB2BGR)
    text_block = cv2.cvtColor(np.uint8(new_image), cv2.COLOR_RGB2BGR)

    return text_block.copy()


def generate_incremental_character_animations(image, fpw, line_text, height_offset, width_boundary, text_font, text_colour, dash = False):
    """
    This function takes:
        - An input image
        - FPW (the number of frames for each character)
        - A line of text, 
        - The height offset, 
        - The width boundary
        - The text font 
        - The text colour
        - A dash flag - this indicates if were going to add a vertical dash between each frame

    And then creates an animation for the line_text where each character appears incrementally.
    """
    
    tot_frames = int(len(line_text)*fpw)
    
    for i in range(fpw):
        yield image.copy()
        
    for i in range(tot_frames):
        ind = int(i/fpw) + 1
        text = line_text[0:ind]
        if dash == True:
            if i % 2 == 0:
                text = '%s|' % text # this adds a dash to every other frame in the animation
        txt_block = generate_one_liner(image, text, height_offset, width_boundary, text_font, text_colour, colour_alpha=255)
        new_img = cv2.cvtColor(np.uint8(txt_block.copy()), cv2.COLOR_RGB2BGR)
        yield new_img
        

def generate_incremental_word_animations(image, line_text, height_offset, width_boundary, text_font, text_colour, voiceover_type):
#     """
#     This function takes a piece of text, a height offset, font and other parameters and creates an animation where each word
#     in the line_text gradually appears over the input image

    """
    ** Note: You will need to work on this later by adding the slide_word_dict that allows you to find the number of frames
             for each word. 
    This function takes:
        - An input image
        - A line of text, 
        - The height offset, 
        - The width boundary
        - The text font 
        - The text colour
        - voiceover_type - The type of voiceover which will determine how the word_frames are structured

    And then creates an animation for the line_text where each word appears incrementally.
    """
    
    if voiceover_type == 'No Voice':
        word_frames = 5 
    else:
        pass # In this case you will need to pass in a word dictionary that dictates how many frames for each word
    
    # Split the line text into individual words
    tokens = line_text.split()

    for i in range(len(tokens)+1):
        text = ' '.join(tokens[0:i])
        txt_block = generate_one_liner(image, text, height_offset, width_boundary, text_font, text_colour, colour_alpha=255)
        
        for t in range(word_frames):
            new_img = cv2.cvtColor(np.uint8(txt_block.copy()), cv2.COLOR_RGB2BGR)
            yield new_img
            
            
def generate_multiline_text_animations(voiceover_type, line_list, image, text_font, text_colour, height_offset, max_height, adjustment, animation_type, width_mult = 0.1, fpw = 1, dash = False):
    """
    This loops through the lines of the long string of text that have been split into individual lines and then generates
    an animation to add them to the image line-by-line
    """
    ## Create dummy variables for the y_anim_offset and y_offset since we wont be using them
    y_anim_offset = 0
    y_offset = 0
    
    # Calculate the number of lines in the keypoints
    num_lines = len(line_list)
    
#     line_count = 0
#     for i in range(len(line_list)):
#         if line_list[i] != 'NA':
#             line_count += 1
    
    for i in range(len(line_list)):
        line_text = line_list[i]
        line_h = height_offset + (max_height*i)
        
        if adjustment == 'Center':
            # Calculate the width boundary
            line_w,temp_h = text_font.getsize(line_text)
            txt_offset = line_w+len(line_text)
            width_boundary = int((image.shape[1] - txt_offset)/2)
            input_image = get_line_text_image(i, image, line_list, max_height, height_offset, text_font, text_colour, adjustment, y_anim_offset, y_offset, width_mult, 
                                              colour_alpha=255)

        elif adjustment == 'Left':
            width_boundary = int(image.shape[1]*width_mult)
            input_image = get_line_text_image(i, image, line_list, max_height, height_offset, text_font, text_colour, adjustment, y_anim_offset, y_offset, width_mult,
                                             colour_alpha=255)
        
        if animation_type == 'Character':
            text_animation_frames = generate_incremental_character_animations(input_image, fpw, line_text, line_h, width_boundary, text_font, text_colour, dash)
            
        elif animation_type == 'Word':
            text_animation_frames = generate_incremental_word_animations(input_image, line_text, line_h, width_boundary, text_font, text_colour, voiceover_type)
            
        yield text_animation_frames

    if dash == True:
        ## Add a final piece here that yields the final output image. This is mainly for when you have the dash animation to ensure that the final image is appropriate
        y_anim_offset = 0
        y_offset = 0
        output_img = get_line_text_image(num_lines, image, line_list, max_height, height_offset, text_font, text_colour, adjustment, y_anim_offset, y_offset, width_mult, colour_alpha=255)
        yield [output_img]
        
        
"""
*********************************************************************************************************
This is the end of functions created as at 01/01/2021. Any new functions created should come under these. 
Do this for all functions going forward.
*********************************************************************************************************
"""

def generate_one_liner_incrementally(line, line_h, width_boundary, image, text_font, colour):
    """
    This takes a single line of a headline and incrementally adds each character into the headline image.
    *** Move this to the anim_utils when you are doing cleanups after concluding work on template 1.
    """
    
    temp = image.copy()
    temp = Image.fromarray(temp)
    draw = ImageDraw.Draw(temp)

    draw.text(((width_boundary,line_h)), line, fill=colour, font=text_font) 

    new_image = temp.convert("RGB")
    text_block = cv2.cvtColor(np.uint8(new_image), cv2.COLOR_RGB2BGR)
    text_block = cv2.cvtColor(np.uint8(new_image), cv2.COLOR_RGB2BGR)

    return text_block.copy()


def calculate_optimal_font_size(text, text_threshold, max_font_size, width_limit, font_path, max_num_lines, max_total_height, num_paragraphs):
    """
    This function takes the maximum font size, as well as the intended text and font dict and 
    then calculates the recommended font size based on the max_num_lines. This essentially dynamically
    resizes the text based on its length
    ** Note that we are making the assumption here that the max_height is always 1.2 * font_size
    """
    # Shorten the text based on the text_threshold
    shortened_text = shorten_text(text, text_threshold) 

    # Initialise the initial_font_dict
    initial_font_dict = {
        'Font Size':max_font_size,
        'Width Limit':width_limit,
        'Font Path':'%s' % font_path
    }

    # Split the text into individual lines based on the initial_font_dict
    text_line_list = split_text(text, initial_font_dict)
    
    # Check if the length of the text_line_list is less than or equal to max_num_lines, if not
    # then we will need to dynamically reduce the font size until it fits the criteria
    temp_font_size = max_font_size
    max_height = 1.2 * temp_font_size
    num_lines = len(text_line_list)
    text_total_height = max_height * (num_lines + num_paragraphs)
    if (num_lines > max_num_lines) or (text_total_height > max_total_height):
        for i in range(10):
            sub = (i+1)*5 # number to iteratively subtract from the baseline font size
            temp_font_size = max_font_size - sub
            font_dict = {
                'Font Size':temp_font_size,
                'Width Limit':width_limit,
                'Font Path':'%s' % font_path
            }
            temp_text_line_list = split_text(shortened_text, font_dict)
            temp_num_lines = len(temp_text_line_list)
            max_height = 1.2 * temp_font_size
            temp_text_total_height = max_height * (temp_num_lines + num_paragraphs)
            if (temp_num_lines <= max_num_lines) and (temp_text_total_height < max_total_height):
                break
    else:
        font_dict = {
            'Font Size':temp_font_size,
            'Width Limit':width_limit,
            'Font Path':'%s' % font_path
        }

    return font_dict, temp_font_size


# Change the input to be the story params so its not too long
def generate_line_text_fade_in_animations(image, line_list, height_offset, max_height, text_font, text_colour, text_adjustment, width_offset_multiple, y_anim_offset, FPS, 
                                          text_fade_in_frames, text_fade_out_frames, voiceover_type, audio_len):
    """
    This function creates an animation where the text all appears at once by fading in from the top, and then afterwards, starts to fade out downwards.
    """
    # 1 - Calculate the fade_in and fade_out steps
    fade_in_alpha_steps = int(255/(text_fade_in_frames-1)) 
    fade_in_y_steps = round(y_anim_offset/(text_fade_in_frames-1),2) 
    fade_out_alpha_steps = int(255/(text_fade_out_frames-1)) 
    fade_out_y_steps = round(y_anim_offset/(text_fade_out_frames-1),2) 

    # 2 - Set the line_ind so that all the available text appears
    line_ind = len(line_list)
    
    ## 3 - First create the fade-in animations
    # 3a - Generate the colour alpha and y steps for the fade-in sequence
    colour_alpha_list = []
    y_offset_list = []
    for i in range(text_fade_in_frames-1):
        colour_alpha = int(fade_in_alpha_steps * i)
        y_offset = int(fade_in_y_steps * i)
        colour_alpha_list.append(colour_alpha)
        y_offset_list.append(y_offset)
        
    # 3b - Add the actual values at the end
    colour_alpha_list.append(255)
    y_offset_list.append(y_anim_offset)
    
    # 3c - Generate the fade-in animations
    for i in range(text_fade_in_frames):
        colour_alpha = colour_alpha_list[i]
        y_offset = y_offset_list[i]
        output_img = get_line_text_image(line_ind, image.copy(), line_list, max_height, height_offset, text_font, text_colour, text_adjustment, 
                                                y_anim_offset, y_offset, width_offset_multiple, colour_alpha)
        yield output_img
    
    # 3d - Get the last frame of the fade-in animation
    last_frame = get_line_text_image(line_ind, image.copy(), line_list, max_height, height_offset, text_font, text_colour, text_adjustment, y_anim_offset= 0, 
                                     y_offset = 0, width_mult=width_offset_multiple, colour_alpha=255)
    
    if voiceover_type != 'No Voice':
        # 3e - Hold the last frame in line for a number of frames based on the length of the line list
        hold_frames = int(audio_len*FPS)
    else:
        # 3e - Hold the last frame in line for a number of frames based on the length of the line list
        if line_ind > 5:
            hold_frames = int(0.7*line_ind*FPS) 
        else:
            hold_frames = int(0.8*line_ind*FPS) 
    
    # Add hold frames to the animation
    for i in range(hold_frames):
        yield last_frame
    
    ## 4 - Now create the fade out animation
    fade_out_y_anim_offset = 0
    
    # 4a - Generate the colour alpha and y steps for the fade-out sequence
    colour_alpha_list = []
    y_offset_list = []
    for i in range(text_fade_out_frames-1):
        colour_alpha = int(fade_out_alpha_steps * i)
        y_offset = int(fade_out_y_steps * i) * -1
        colour_alpha_list.append(colour_alpha)
        y_offset_list.append(y_offset)

    # 4b - Add the actual values at the end
    colour_alpha_list.append(255)
    colour_alpha_list.reverse() # reverse the list so that the text gradually fades out
    y_offset_list.append(y_anim_offset * -1)
    
    # 4c - Generate the fade-out animations
    for i in range(text_fade_out_frames):
        colour_alpha = colour_alpha_list[i]
        y_offset = y_offset_list[i]
        output_img = get_line_text_image(line_ind, image.copy(), line_list, max_height, height_offset, text_font, text_colour, text_adjustment, 
                                                fade_out_y_anim_offset, y_offset, width_offset_multiple, colour_alpha)
        yield output_img