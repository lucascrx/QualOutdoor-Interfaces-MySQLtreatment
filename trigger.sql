DROP PROCEDURE IF EXISTS proc_tree; 

DELIMITER //

CREATE PROCEDURE proc_tree(IN table_name VARCHAR(100))
BEGIN	
	/*On récupère le nom de la table à transformer en arbre*/
	DECLARE table_upload VARCHAR(100) DEFAULT table_name;
	/* on se sert de ce nom pour generer le nom de la future table contennant l'arbre associé*/
	DECLARE table_tree VARCHAR(100) DEFAULT CONCAT( 'tree_' , table_upload );/* @table_upload);*/
	
	/*déclaration des variables qui recupèrent les champs transmis par le curseur
	Ce sont des variables systemes necessaires pour recuperer les champs du curseur*/
	DECLARE curs_line , curs_ref , curs_lvl INT;
	DECLARE curs_lat , curs_lng INT;
	DECLARE curs_data VARCHAR(100); 
	

	/*préparation de son handler associé*/
	DECLARE done INT DEFAULT 0;
	
	
	/*nom de la table temporaire qui stocke les bords droits de chaque dernier noeud mis dans l'arbre
	pour chaque étage*/
	DECLARE bord_droits_temp VARCHAR(100) DEFAULT 'TABLE_BORDS_DROITS';
	

	/*préparation d'un curseur qui récupère tous les champs de la table qui a révéillé le trigger*/
	DECLARE curs CURSOR FOR SELECT  line , reference , lvl , lat , lng , meas_data FROM select_view;
	
	DECLARE CONTINUE handler FOR NOT FOUND SET done = 1;
	
	
	/*INITIALISATION DE LA VUE SUR LA TABLE */
	
	SET @strV1 = CONCAT('DROP VIEW IF EXISTS select_view' ); 
	PREPARE stmV1 FROM @strV1;
	EXECUTE stmV1;
	DEALLOCATE PREPARE stmV1;
	
	
	SET @strV2 = CONCAT('CREATE VIEW select_view AS SELECT * FROM ' , table_upload , ' WHERE reference != 0' ); 
	PREPARE stmV2 FROM @strV2;
	EXECUTE stmV2;
	DEALLOCATE PREPARE stmV2;
	
	/*INITIALISATION DE LA NOUVELLE TABLE : REPRESENTATION INTERVALLAIRE*/
	SET @str1 = CONCAT('CREATE TABLE ' , table_tree , ' ( ls INT NOT NULL , rs INT NOT NULL , reference INT) ENGINE=InnoDB ');
	PREPARE stm1 FROM @str1;
	EXECUTE stm1;
	DEALLOCATE PREPARE stm1;
	
	
	/*on stocke dans une table temporaire les bords droits de chaque dernier noeud mis dans l'arbre*/
	/*pour chaque étage*/
	DROP TEMPORARY TABLE IF EXISTS bords_droits_temp;/*TABLE A SUPPRIMER EN FIN D'EXECUTION!!!*/
	CREATE TEMPORARY TABLE bords_droits_temp ( lvl INT PRIMARY KEY , rs INT ) ENGINE=memory;
	/*initialisation des lignes de la table*/
	INSERT INTO bords_droits_temp ( lvl) VALUES (0),(1),(2),(3),(4),(5),(6),(7);
	
	/*initialisation de la variable correspondant au bord gauche affecté à la ligne précédente dans la table*/
	SET @bordG = 0;
	/*initialisation de la variable correspondant au niveau dans l'arbre de la ligne précédente dans la table*/
	SET @prev_lvl = 0;
	
	/*lecture des resultats du curseur*/
	OPEN curs;
		/*boucle sur les lignes relevées par le curseur*/
		insert_node : LOOP
			/*on recupere dans les variables les champs captés par le curseur*/

			FETCH curs INTO curs_line , curs_ref , curs_lvl , curs_lat , curs_lng , curs_data;
			
			/*On affecte les variables systèmes aux variables user car elles sont plus maniables d'utilisation
			de plus les variables systemes ne sont pas prises en compte dans les statements*/
			SET @curs_lvl_temp = curs_lvl;
			SET @curs_lat_temp = curs_lat;
			SET @curs_lng_temp = curs_lng;
			SET @curs_line_temp = curs_line;
			SET @curs_ref_temp = curs_ref;
			SET @curs_data_temp = curs_data;
			/*Variable representant la largeure de l'intervalle correspondant à la ligne courante de la table*/
			SET @larg_temp = NULL;
			

			/*si il n'y a plus de ligne a relever : fin de la boucle*/
			IF done THEN
				LEAVE insert_node;
			END IF;
			/*on initialise la largeur du noeud à 1*/
			SET @larg_temp = 1;
			/*si on lit une feuille : on l'insert dans la table de data*/
			IF curs_lvl = 7 THEN
				PREPARE stm2 FROM 'INSERT INTO table_data ( DATE , LAT , LNG , DAT) VALUES ( @curs_ref_temp , @curs_lat_temp , @curs_lng_temp , @curs_data_temp )';
				EXECUTE stm2;
				/*on récupère l'ID d'insertion pour mettre a jour curs_ref*/
				PREPARE stm2 FROM 'SET @curs_ref_temp = LAST_INSERT_ID()';
				EXECUTE stm2;
			
			ELSE /*si on lit un noeud non feuille il faut determiner la largeur de l'intervalle associé*/
			
			/*Pour cela on crée une vue*/
				PREPARE stm3 FROM 'DROP VIEW IF EXISTS select_view2' ;
				EXECUTE stm3;
				DEALLOCATE PREPARE stm3;
				
			/*Cette vue est une extraction de la table d'entrée qui selectionne les lignes d'index strict. superieure
			à la ligne courante.*/
				SET @str4 = CONCAT('CREATE VIEW select_view2 AS SELECT * FROM ' , table_upload , ' WHERE line > ' , curs_line  , ' ORDER BY line ASC ' ); 
				PREPARE stm4 FROM @str4;
				EXECUTE stm4;
				DEALLOCATE PREPARE stm4;
		
		
				/*On repere dans cette vue toutes les lignes dont l'index est inferieur à la première ligne de niveau 
				égal à la ligne courante : ce sont les fils (pas forcements directs) du noeud correspondant à la ligne
				Si cette ligne n'est pas trouvé : toutes lignes des la table representent des fils */
				SET @str5 = CONCAT('SELECT count(*) INTO @larg_temp FROM select_view2 where  line < COALESCE((SELECT line FROM select_view2 WHERE lvl <=  @curs_lvl_temp AND line > @curs_line_temp   ORDER BY line ASC LIMIT 1) , (SELECT count(*) FROM select_view2 ) + @curs_line_temp +2)' ); 
				PREPARE stm5 FROM @str5;
				EXECUTE stm5;
				DEALLOCATE PREPARE stm5;
			
			
				/*On déduit la largeur de l'intervalle associé en fonction du nombre de fils*/
				PREPARE stm6 FROM 'SET @larg_temp = 2*@larg_temp+1 ';
				EXECUTE stm6;	
				DEALLOCATE PREPARE stm6;
				
			END IF;
			
			/*determination de l'index d'insertion dans l'arbre*/
			
			/* si on insere un fils de la ligne précédente*/
			IF (@prev_lvl = (@curs_lvl_temp - 1)) THEN
				/*on l'insere dans le coin gauche du père*/
				SET @str7 = 'SET @bordG = @bordG + 1';
				PREPARE stm7 FROM @str7;
				EXECUTE stm7;
				DEALLOCATE PREPARE stm7;
			ELSE /*sinon il s'agit d'un frere ou d'un oncle de la ligne précédente
				/*on regarde le bord droit du frère : un frère existe forcément et il sera le dernier ayant modifé la table bord_droit_temp*/
				/*on se cale donc par rapport à bord droit du frere */
				PREPARE stm8 FROM 'SELECT rs INTO @bordG FROM bords_droits_temp WHERE lvl = @curs_lvl_temp'; 
				EXECUTE stm8;
				DEALLOCATE PREPARE stm8;
				/*on se décale d'une unité par rapport au frere*/
				PREPARE stm9 FROM 'SET @bordG = @bordG + 1';
				EXECUTE stm9;
				DEALLOCATE PREPARE stm9;
			END IF;
			/*on a maintenant tous les éléments pour inserer:*/
			
			/*on determine l'index du bord droit en fonction de la largeur et de l'index du bord gauche*/
			PREPARE stm10 FROM 'SET @bordD = @bordG + @larg_temp';
			EXECUTE stm10;
			DEALLOCATE PREPARE stm10;
			
			/*on insere les valeurs determinées dans la nouvelle table de représentation intervallaire*/
			SET @str11 = CONCAT('INSERT INTO ' , table_tree , ' ( ls , rs , reference ) VALUES ( @bordG , @bordD , @curs_ref_temp)');
			PREPARE stm11 FROM @str11;
			EXECUTE stm11;
			DEALLOCATE PREPARE stm11;
			
			/*mise a jour des parametres pour l'insertion de la prochaine ligne*/
			
			PREPARE stm12 FROM 'SET @prev_lvl = @curs_lvl_temp';
			EXECUTE stm12;
			DEALLOCATE PREPARE stm12;
			
			PREPARE stm13 FROM 'UPDATE bords_droits_temp SET rs = @bordD WHERE lvl = @curs_lvl_temp';
			EXECUTE stm13;
			DEALLOCATE PREPARE stm13;
			
			

		END LOOP insert_node;
	CLOSE curs;
	/*on peut supprimer la table temporaire*/
	PREPARE stm14 FROM 'DROP TABLE bords_droits_temp';
	EXECUTE stm14;
	DEALLOCATE PREPARE stm14;
	
	/*Une fois l'arbre généré on peut supprimer la premiere table:*/
	SET @str15 = CONCAT('DROP TABLE ' , table_upload ); 
	PREPARE stm15 FROM @str15;
	EXECUTE stm15;
	DEALLOCATE PREPARE stm15;
	
END
	
			

		
