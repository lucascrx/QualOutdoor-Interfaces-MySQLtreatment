/*Autre algorithme qui se revele moins rapide que la premiere version*/

DROP PROCEDURE IF EXISTS proc_tree; 

DELIMITER //

CREATE PROCEDURE proc_tree_v2(IN table_name VARCHAR(100))
BEGIN	
	/*On récupère le nom de la table à transformer en arbre*/
	DECLARE table_upload VARCHAR(100) DEFAULT table_name;
	/* on se sert de ce nom pour generer le nom de la future table contennant l'arbre associé*/
	DECLARE table_tree VARCHAR(100) DEFAULT CONCAT( 'tree_' , table_upload );
	
	/*déclaration des variables qui recupèrent les champs transmis par le curseur
	Ce sont des variables systemes necessaires pour recuperer les champs du curseur*/
	DECLARE curs_line , curs_ref , curs_lvl INT;
	DECLARE curs_lat , curs_lng INT;
	DECLARE curs_data VARCHAR(100); 
	

	/*préparation de son handler associé*/
	DECLARE done INT DEFAULT 0;
	
	
	/*nom de la table temporaire qui stocke les bords droits de chaque dernier noeud mis dans l'arbre
	pour chaque étage*/
	DECLARE reference_temp VARCHAR(100) DEFAULT 'TABLE_REFERENCE_TEMP';
	
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
	
	/*on stocke dans une table temporaire la reference du dernier noeud insere pour chaque étage de l'arbre*/
	DROP TEMPORARY TABLE IF EXISTS reference_temp;/*TABLE A SUPPRIMER EN FIN D'EXECUTION*/
	CREATE TEMPORARY TABLE reference_temp ( lvl INT PRIMARY KEY , reference INT ) ENGINE=memory;
	/*initialisation des lignes de la table*/
	INSERT INTO reference_temp ( lvl) VALUES (0),(1),(2),(3),(4),(5),(6),(7);
	
	/*initialisation de la variable correspondant à l'index d'insertion courant dans la nouvelle table*/
	SET @idx = 0;
	/*initialisation de la variable correspondant au niveau dans l'arbre de la ligne précédente dans la table*/
	SET @prev_lvl = 0;
	/*reference au frere*/
	SET @brother_ref = 0;
	
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
			
			IF done THEN
				LEAVE insert_node;
			END IF;
			
			/*si on lit une feuille : on l'insere dans la table de data*/
			
			IF curs_lvl = 7 THEN
				PREPARE stm2 FROM 'INSERT INTO table_data ( DATE , LAT , LNG , DAT) VALUES ( @curs_ref_temp , @curs_lat_temp , @curs_lng_temp , @curs_data_temp )';
				EXECUTE stm2;
				/*on récupère l'ID d'insertion pour mettre a jour curs_ref*/
				PREPARE stmt2 FROM 'SET @curs_ref_temp = LAST_INSERT_ID()';
				EXECUTE stmt2;
			END IF;
			
			/*On determine l'index d'insertion*/
			
			/* si on insere un fils de la ligne précédente*/
			IF (@prev_lvl = (@curs_lvl_temp - 1)) THEN
				/*on l'insere dans le coin gauche du père*/
				SET @str7 = 'SET @idx = @idx + 1';
				PREPARE stm7 FROM @str7;
				EXECUTE stm7;
				DEALLOCATE PREPARE stm7;
			ELSE /*sinon il s'agit d'un frere ou d'un oncle de la ligne précédente
				/*on regarde donc la table reference temp ou se trouve forcement un 
				frère du noeud correspondant à la ligne courante */
				PREPARE stm8 FROM 'SELECT reference INTO @brother_ref FROM reference_temp WHERE lvl = @curs_lvl_temp'; 
				EXECUTE stm8;
				DEALLOCATE PREPARE stm8;
				/*une fois que le frere est resolu on consulte le nouvel arbre pour avoir le bord droit de son intervalle associé*/
				SET @str9 = CONCAT('SELECT rs INTO @idx FROM ' , table_tree , ' WHERE reference = @brother_ref ORDER BY rs DESC LIMIT 1' );
				PREPARE stm9 FROM @str9;
				EXECUTE stm9;
				DEALLOCATE PREPARE stm9;
				
				/*on se décale d'une unité par rapport au frere*/
				PREPARE stm10 FROM 'SET @idx = @idx + 1';
				EXECUTE stm10;
				DEALLOCATE PREPARE stm10;			
			END IF;
	
			/*Insertion du nouveau noeud dans l'abre*/
			
			/*On prepare la place pour le nouveau noeud*/
				SET @str11 = CONCAT('UPDATE ' , table_tree , ' SET ls = ls + 2 WHERE ls >= @idx' );
				PREPARE stm11 FROM @str11;
				EXECUTE stm11;
				DEALLOCATE PREPARE stm11;
				
				SET @str12 = CONCAT('UPDATE ' , table_tree , ' SET rs = rs + 2 WHERE rs >= @idx' );
				PREPARE stm12 FROM @str12;
				EXECUTE stm12;
				DEALLOCATE PREPARE stm12;
				
				PREPARE stm13 FROM 'SET @bordD = @idx + 1';
				EXECUTE stm13;
				DEALLOCATE PREPARE stm13;
				
			/*on insere les valeurs determinées dans la nouvelle table de représentation intervallaire*/
				SET @str13 = CONCAT('INSERT INTO ' , table_tree , ' ( ls , rs , reference ) VALUES ( @idx , @bordD , @curs_ref_temp)');
				PREPARE stm13 FROM @str13;
				EXECUTE stm13;
				DEALLOCATE PREPARE stm13;


			/*mise a jour des parametres pour l'insertion de la prochaine ligne*/
			PREPARE stm12 FROM 'SET @prev_lvl = @curs_lvl_temp';
			EXECUTE stm12;
			DEALLOCATE PREPARE stm12;
			
			PREPARE stm13 FROM 'UPDATE reference_temp SET reference = @curs_ref_temp WHERE lvl = @curs_lvl_temp';
			EXECUTE stm13;
			DEALLOCATE PREPARE stm13;
			
		END LOOP insert_node;
	CLOSE curs;
	
		/*on peut supprimer la table temporaire*/
	PREPARE stm14 FROM 'DROP TABLE reference_temp';
	EXECUTE stm14;
	DEALLOCATE PREPARE stm14;
	
		/*Une fois l'arbre généré on peut supprimer la premiere table:*/
	SET @str15 = CONCAT('DROP TABLE ' , table_upload ); 
	PREPARE stm15 FROM @str15;
	EXECUTE stm15;
	DEALLOCATE PREPARE stm15;


END
