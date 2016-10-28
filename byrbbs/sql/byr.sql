/*
Navicat MySQL Data Transfer

Source Server         : 192.168.1.98
Source Server Version : 50173
Source Host           : 192.168.1.98:3306
Source Database       : byr

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2016-10-28 17:48:24
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for board
-- ----------------------------
DROP TABLE IF EXISTS `board`;
CREATE TABLE `board` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `board_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `board_name` (`board_name`)
) ENGINE=MyISAM AUTO_INCREMENT=279 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for comment
-- ----------------------------
DROP TABLE IF EXISTS `comment`;
CREATE TABLE `comment` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `post_id` char(16) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `url` varchar(80) DEFAULT NULL,
  `content` text,
  `user_id` varchar(50) DEFAULT NULL,
  `user_name` varchar(50) DEFAULT NULL,
  `publish_time` datetime DEFAULT NULL,
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `post_id` (`post_id`),
  KEY `url` (`url`) USING BTREE,
  KEY `user_id` (`user_id`) USING BTREE,
  KEY `user_name` (`user_name`) USING BTREE,
  KEY `publish_time` (`publish_time`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=21965416 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for data
-- ----------------------------
DROP TABLE IF EXISTS `data`;
CREATE TABLE `data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_name` varchar(20) NOT NULL,
  `data_value` longtext NOT NULL,
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for post
-- ----------------------------
DROP TABLE IF EXISTS `post`;
CREATE TABLE `post` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `post_id` char(16) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `url` varchar(80) DEFAULT NULL,
  `content` text,
  `user_id` varchar(50) DEFAULT NULL,
  `user_name` varchar(50) DEFAULT NULL,
  `board_name` varchar(30) DEFAULT NULL,
  `post_num` int(11) DEFAULT NULL,
  `publish_time` datetime DEFAULT NULL,
  `last_time` datetime DEFAULT NULL,
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`,`post_id`),
  KEY `title` (`title`) USING BTREE,
  KEY `url` (`url`) USING BTREE,
  KEY `user_id` (`user_id`) USING BTREE,
  KEY `user_name` (`user_name`) USING BTREE,
  KEY `board_name` (`board_name`) USING BTREE,
  KEY `post_num` (`post_num`) USING BTREE,
  KEY `publish_time` (`publish_time`) USING BTREE,
  KEY `last_time` (`last_time`) USING BTREE,
  KEY `insert_time` (`insert_time`) USING BTREE,
  KEY `post_id` (`post_id`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=1229929 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for post_index
-- ----------------------------
DROP TABLE IF EXISTS `post_index`;
CREATE TABLE `post_index` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `word` varchar(255) NOT NULL,
  `doc_fre` int(11) DEFAULT NULL,
  `list` longtext,
  PRIMARY KEY (`id`),
  KEY `word` (`word`)
) ENGINE=MyISAM AUTO_INCREMENT=374165 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for site
-- ----------------------------
DROP TABLE IF EXISTS `site`;
CREATE TABLE `site` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `site` varchar(50) DEFAULT NULL,
  `country_cn` varchar(50) DEFAULT NULL,
  `country_en` varchar(50) DEFAULT NULL,
  `province` varchar(50) DEFAULT NULL,
  `ip` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=28986 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for stop_word
-- ----------------------------
DROP TABLE IF EXISTS `stop_word`;
CREATE TABLE `stop_word` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `word` varchar(255) DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2742 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(14) NOT NULL,
  `post_num` int(11) DEFAULT NULL,
  `comment_num` int(11) DEFAULT NULL,
  `user_name` varchar(40) DEFAULT NULL,
  `gender` varchar(10) DEFAULT NULL,
  `astro` varchar(10) DEFAULT NULL,
  `qq` varchar(100) DEFAULT NULL,
  `msn` varchar(100) DEFAULT NULL,
  `home_page` varchar(100) DEFAULT NULL,
  `level` varchar(10) DEFAULT NULL,
  `post_count` int(11) DEFAULT NULL,
  `score` int(11) DEFAULT NULL,
  `life` int(11) DEFAULT NULL,
  `last_login_time` datetime DEFAULT NULL,
  `last_login_ip` varchar(50) DEFAULT NULL,
  `last_login_site` varchar(20) DEFAULT NULL,
  `country_cn` varchar(50) DEFAULT NULL,
  `country_en` varchar(50) DEFAULT NULL,
  `province` varchar(50) DEFAULT NULL,
  `last_login_bupt` varchar(20) DEFAULT NULL,
  `status` varchar(30) DEFAULT NULL,
  `face_url` varchar(80) DEFAULT NULL,
  `face_height` float DEFAULT NULL,
  `face_width` float DEFAULT NULL,
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`) USING BTREE,
  KEY `post_num` (`post_num`) USING BTREE,
  KEY `comment_num` (`comment_num`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=119003 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for user_id
-- ----------------------------
DROP TABLE IF EXISTS `user_id`;
CREATE TABLE `user_id` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=24531726 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for user_id_exsit
-- ----------------------------
DROP TABLE IF EXISTS `user_id_exsit`;
CREATE TABLE `user_id_exsit` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=107963 DEFAULT CHARSET=utf8;
