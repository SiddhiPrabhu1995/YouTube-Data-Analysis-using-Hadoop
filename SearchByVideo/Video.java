/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package searchbyvideo;

public class Video {
    
    final String videoID;

    public Video(String videoID) {
        this.videoID = videoID;
    }

    @Override
    public String toString() {
        return this.videoID;
    }
	
	
    
}
